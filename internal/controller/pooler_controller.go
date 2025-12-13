/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/pooler/secrets"
)

// PoolerReconciler reconciles a Pooler object
type PoolerReconciler struct {
	client.Client
	DiscoveryClient discovery.DiscoveryInterface
	Scheme          *runtime.Scheme
	Recorder        record.EventRecorder
	SecretUpdater   secrets.SecretUpdater
}

// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=poolers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=poolers/status,verbs=get;update;patch;watch
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=poolers/finalizers,verbs=update
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;watch;delete;patch
// +kubebuilder:rbac:groups="",resources=secrets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;create;delete;update;patch;list;watch
// +kubebuilder:rbac:groups="apps",resources=deployments,verbs=get;create;delete;update;patch;list;watch

// Reconcile implements the main reconciliation loop for pooler objects
func (r *PoolerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	contextLogger, ctx := log.SetupLogger(ctx)



	var pooler apiv1.Pooler
	if err := r.Get(ctx, req.NamespacedName, &pooler); err != nil {
		// This also happens when you delete a Pooler resource in k8s. If
		// that's the case, let's just wait for the Kubernetes garbage collector
		// to remove all the Pods of the cluster.
		if apierrs.IsNotFound(err) {
	
			// Try to handle cleanup for any remaining clusters that might need secret reversion
			if err := r.handleOrphanedPoolerCleanup(ctx, req.NamespacedName); err != nil {
				contextLogger.Error(err, "Failed to handle orphaned pooler cleanup")
			}
			return ctrl.Result{}, nil
		}

		// This is a real error, maybe the RBAC configuration is wrong?
		contextLogger.Error(err, "Failed to get pooler resource", "pooler", req.Name)
		return ctrl.Result{}, fmt.Errorf("cannot get the pooler resource: %w", err)
	}



	// Handle finalizer for proper cleanup
	const finalizerName = "pooler.cnpg.io/secret-cleanup"
	
	// Check if pooler is being deleted
	if !pooler.DeletionTimestamp.IsZero() {
		contextLogger.Info("Pooler is being deleted, handling cleanup", "pooler", pooler.Name)
		
		// Perform cleanup before removing finalizer
		if err := r.handlePoolerDeletionCleanup(ctx, &pooler); err != nil {
			contextLogger.Error(err, "Failed to handle pooler deletion cleanup")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		
		// Remove our finalizer to allow deletion to proceed
		if controllerutil.ContainsFinalizer(&pooler, finalizerName) {
			controllerutil.RemoveFinalizer(&pooler, finalizerName)
			if err := r.Update(ctx, &pooler); err != nil {
				contextLogger.Error(err, "Failed to remove finalizer")
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}

		}
		
		return ctrl.Result{}, nil
	}
	
	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(&pooler, finalizerName) {
		controllerutil.AddFinalizer(&pooler, finalizerName)
		if err := r.Update(ctx, &pooler); err != nil {
			contextLogger.Error(err, "Failed to add finalizer")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		return ctrl.Result{Requeue: true}, nil
	}

	// We make sure that there isn't a cluster with the same name as the pooler
	conflictingCluster, err := getClusterOrNil(ctx, r.Client, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("while getting cluster resource: %w", err)
	}

	if conflictingCluster != nil {
		r.Recorder.Event(
			&pooler,
			"Warning",
			"NameClash",
			"Name clash between Pooler and Cluster detected, resource reconciliation skipped")
		return ctrl.Result{}, nil
	}

	// Get the set of resources we directly manage and their status
	resources, err := r.getManagedResources(ctx, &pooler)
	if err != nil {
		contextLogger.Error(err, "Failed to get managed resources")
		return ctrl.Result{}, fmt.Errorf("while getting managed resources: %w", err)
	}

	// Early exit if some required prerequisite resources are not yet available
	if res := r.waitForPrerequisites(ctx, &pooler, resources); res != nil {
		contextLogger.Debug("Prerequisites not met, waiting", "requeue", res.RequeueAfter)
		return *res, nil
	}

	if res := r.ensureManagedResourcesAreOwned(ctx, pooler, resources); !res.IsZero() {
		contextLogger.Debug("Ownership issues detected, requeuing")
		return res, nil
	}

	// Update the status of the Pooler resource given what we read
	// from the controlled resources
	if err := r.updatePoolerStatus(ctx, &pooler, resources); err != nil {
		if apierrs.IsConflict(err) {
			// Requeue a reconciliation loop since the resource
			// changed while we were synchronizing it

			return ctrl.Result{Requeue: true}, nil
		}
		contextLogger.Error(err, "Failed to update pooler status")
	}

	// Take the required actions to align the spec with the collected status
	if err := r.updateOwnedObjects(ctx, &pooler, resources); err != nil {
		contextLogger.Error(err, "Failed to update owned objects")
		return ctrl.Result{}, err
	}

	// Reconcile cluster secrets for pooler usage
	if err := r.reconcileClusterSecrets(ctx, &pooler); err != nil {
		contextLogger.Error(err, "Failed to reconcile cluster secrets")
		
		// Handle conflicts with immediate requeue
		if apierrs.IsConflict(err) {
			contextLogger.Debug("Conflict detected, requeuing immediately")
			return ctrl.Result{Requeue: true}, nil
		}
		
		// For other errors, requeue with delay
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager setup this controller inside the controller manager
func (r *PoolerReconciler) SetupWithManager(mgr ctrl.Manager, maxConcurrentReconciles int) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrentReconciles}).
		For(&apiv1.Pooler{}).
		Named("pooler").
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.mapSecretToPooler()),
			builder.WithPredicates(secretsPoolerPredicate),
		).
		Complete(r)
}

// isOwnedByPoolerKind checks that an object is owned by a pooler and returns
// the owner name
func isOwnedByPoolerKind(obj client.Object) (string, bool) {
	owner := metav1.GetControllerOf(obj)
	if owner == nil {
		return "", false
	}

	if owner.Kind != apiv1.PoolerKind {
		return "", false
	}

	if owner.APIVersion != apiSGVString {
		return "", false
	}

	return owner.Name, true
}

func isOwnedByPooler(poolerName string, obj client.Object) bool {
	ownerName, isOwned := isOwnedByPoolerKind(obj)
	return isOwned && poolerName == ownerName
}

func (r *PoolerReconciler) ensureManagedResourcesAreOwned(
	ctx context.Context,
	pooler apiv1.Pooler,
	resources *poolerManagedResources,
) ctrl.Result {
	contextLogger := log.FromContext(ctx)

	var invalidData []interface{}
	if resources.Deployment != nil && !isOwnedByPooler(pooler.Name, resources.Deployment) {
		invalidData = append(invalidData, "notOwnedDeploymentName", resources.Deployment.Name)
	}

	if resources.Service != nil && !isOwnedByPooler(pooler.Name, resources.Service) {
		invalidData = append(invalidData, "notOwnedServiceName", resources.Service.Name)
	}

	if resources.Role != nil && !isOwnedByPooler(pooler.Name, resources.Role) {
		invalidData = append(invalidData, "notOwnedRoleName", resources.Role.Name)
	}

	if resources.RoleBinding != nil && !isOwnedByPooler(pooler.Name, resources.RoleBinding) {
		invalidData = append(invalidData, "notOwnedRoleBindingName", resources.RoleBinding.Name)
	}

	if len(invalidData) == 0 {
		return ctrl.Result{}
	}

	contextLogger.Error(
		errors.New("invalid ownership for managed resources"),
		"while ensuring managed resources are owned, requeueing...",
		invalidData...,
	)
	r.Recorder.Event(&pooler,
		"Warning",
		"InvalidOwnership",
		"found invalid ownership for managed resources, check logs")

	return ctrl.Result{RequeueAfter: 120 * time.Second}
}

// waitForPrerequisites centralizes the early-return checks for missing dependent resources
// to keep Reconcile lean. It logs a concise message and instructs the controller to
// requeue after a short delay when something is missing.
// Returns a non-nil *ctrl.Result when it requested a requeue; otherwise nil.
func (r *PoolerReconciler) waitForPrerequisites(
	ctx context.Context,
	pooler *apiv1.Pooler,
	resources *poolerManagedResources,
) *ctrl.Result {
	contextLogger := log.FromContext(ctx)
	waitResult := &ctrl.Result{RequeueAfter: 30 * time.Second}

	if resources.Cluster == nil {
		contextLogger.Info("Cluster not found, will retry in 30 seconds",
			"cluster", pooler.Spec.Cluster.Name)
		return waitResult
	}

	// For automated integration, we need AuthUserSecret
	if pooler.IsAutomatedIntegration() && resources.AuthUserSecret == nil {
		contextLogger.Info("AuthUserSecret not found, waiting 30 seconds",
			"secret", pooler.GetAuthQuerySecretName())
		return waitResult
	}

	// For manual TLS authentication to PostgreSQL, we need ServerTLSSecret
	if pooler.GetServerTLSSecretName() != "" && resources.ServerTLSSecret == nil {
		contextLogger.Info("ServerTLSSecret not found, waiting 30 seconds",
			"secret", pooler.GetServerTLSSecretName())
		return waitResult
	}

	// Always required: TLS certificates for accepting client connections
	if resources.ClientTLSSecret == nil {
		contextLogger.Info(
			"ClientTLSSecret not found, waiting 30 seconds",
			"secret", pooler.GetClientTLSSecretNameOrDefault(resources.Cluster))
		return waitResult
	}

	if resources.ClientCASecret == nil {
		contextLogger.Info(
			"ClientCASecret not found, waiting 30 seconds",
			"secret", pooler.GetClientCASecretNameOrDefault(resources.Cluster))
		return waitResult
	}

	if resources.ServerCASecret == nil {
		contextLogger.Info(
			"ServerCASecret not found, waiting 30 seconds",
			"secret", pooler.GetServerCASecretNameOrDefault(resources.Cluster))
		return waitResult
	}

	return nil
}

// mapSecretToPooler returns a function mapping secrets events to the poolers using them
func (r *PoolerReconciler) mapSecretToPooler() handler.MapFunc {
	return func(ctx context.Context, obj client.Object) (result []reconcile.Request) {
		secret, ok := obj.(*corev1.Secret)
		if !ok {
			return nil
		}

		var poolers apiv1.PoolerList

		// get all the clusters handled by the operator in the configmap namespace
		if err := r.List(ctx, &poolers,
			client.InNamespace(secret.Namespace),
		); err != nil {
			log.FromContext(ctx).Error(err, "while getting pooler list for secret",
				"namespace", secret.Namespace, "secret", secret.Name)
			return nil
		}

		// filter the cluster list preserving only the ones which are using
		// the passed secret
		filteredPoolersList := getPoolersUsingSecret(poolers, secret)
		result = make([]reconcile.Request, len(filteredPoolersList))
		for idx, value := range filteredPoolersList {
			result[idx] = reconcile.Request{NamespacedName: value}
		}

		return result
	}
}

// getPoolersUsingSecret get a list of poolers which are using the passed secret
func getPoolersUsingSecret(poolers apiv1.PoolerList, secret *corev1.Secret) (requests []types.NamespacedName) {
	for _, pooler := range poolers.Items {
		if name, ok := isOwnedByPoolerKind(secret); ok && pooler.Name == name {
			requests = append(requests,
				types.NamespacedName{
					Name:      pooler.Name,
					Namespace: pooler.Namespace,
				})
			continue
		}

		if pooler.Spec.PgBouncer != nil && pooler.GetAuthQuerySecretName() == secret.Name {
			requests = append(requests,
				types.NamespacedName{
					Name:      pooler.Name,
					Namespace: pooler.Namespace,
				},
			)
			continue
		}

		if pooler.GetServerTLSSecretName() == secret.Name {
			requests = append(requests,
				types.NamespacedName{
					Name:      pooler.Name,
					Namespace: pooler.Namespace,
				},
			)
			continue
		}
	}
	return requests
}
// reconcileClusterSecrets handles secret updates when pooler state changes
func (r *PoolerReconciler) reconcileClusterSecrets(ctx context.Context, pooler *apiv1.Pooler) error {
	contextLogger := log.FromContext(ctx).WithValues(
		"pooler", pooler.Name,
		"cluster", pooler.Spec.Cluster.Name,
		"namespace", pooler.Namespace,
	)

	if r.SecretUpdater == nil {
		contextLogger.Error(nil, "SecretUpdater is nil - initialization failed!")
		return nil
	}

	// Get the cluster resource
	cluster := &apiv1.Cluster{}
	clusterKey := client.ObjectKey{Name: pooler.Spec.Cluster.Name, Namespace: pooler.Namespace}
	
	if err := r.Get(ctx, clusterKey, cluster); err != nil {
		if apierrs.IsNotFound(err) {
			contextLogger.Info("Cluster not found, skipping secret update")
			return nil
		}
		return fmt.Errorf("failed to get cluster %s: %w", pooler.Spec.Cluster.Name, err)
	}

	// Note: Pooler deletion is now handled by finalizers in the main reconcile loop

	// Update secrets to use this pooler
	if err := r.SecretUpdater.UpdateSecretsForPooler(ctx, cluster, pooler); err != nil {
		contextLogger.Error(err, "Failed to update secrets for pooler")
		r.Recorder.Event(pooler, "Warning", "SecretUpdateFailed", 
			fmt.Sprintf("Failed to update cluster secrets: %v", err))
		return err
	}

	contextLogger.Info("Successfully updated cluster secrets for pooler")
	r.Recorder.Event(pooler, "Normal", "SecretUpdated", 
		"Cluster secrets updated to use pooler service")
	
	return nil
}



// handlePoolerDeletionCleanup handles secret cleanup when a pooler is being deleted
func (r *PoolerReconciler) handlePoolerDeletionCleanup(ctx context.Context, pooler *apiv1.Pooler) error {
	contextLogger := log.FromContext(ctx).WithValues(
		"pooler", pooler.Name,
		"cluster", pooler.Spec.Cluster.Name,
		"namespace", pooler.Namespace,
	)



	// Get the cluster resource
	cluster := &apiv1.Cluster{}
	clusterKey := client.ObjectKey{Name: pooler.Spec.Cluster.Name, Namespace: pooler.Namespace}
	
	if err := r.Get(ctx, clusterKey, cluster); err != nil {
		if apierrs.IsNotFound(err) {
			contextLogger.Info("Cluster not found during cleanup, skipping secret reversion")
			return nil
		}
		return fmt.Errorf("failed to get cluster %s during cleanup: %w", pooler.Spec.Cluster.Name, err)
	}

	// Check if there are other active poolers for this cluster
	activePooler, err := r.SecretUpdater.GetActivePoolerForCluster(ctx, cluster.Name, cluster.Namespace)
	if err != nil {
		return fmt.Errorf("failed to get active pooler for cluster %s during cleanup: %w", cluster.Name, err)
	}

	// Filter out the current pooler being deleted
	if activePooler != nil && activePooler.Name == pooler.Name {
		contextLogger.Info("Active pooler is the one being deleted, looking for alternatives")
		
		// Get all poolers for this cluster
		poolers, err := r.listPoolersForCluster(ctx, cluster.Name, cluster.Namespace)
		if err != nil {
			return fmt.Errorf("failed to list poolers for cluster %s: %w", cluster.Name, err)
		}
		
		// Find an alternative pooler (not the one being deleted)
		var alternativePooler *apiv1.Pooler
		for _, p := range poolers {
			if p.Name != pooler.Name && p.DeletionTimestamp.IsZero() {
				alternativePooler = &p
				break
			}
		}
		
		if alternativePooler != nil {
			// Update secrets to use the alternative pooler
			contextLogger.Info("Found alternative pooler, updating secrets", "alternativePooler", alternativePooler.Name)
			if err := r.SecretUpdater.UpdateSecretsForPooler(ctx, cluster, alternativePooler); err != nil {
				contextLogger.Error(err, "Failed to update secrets for alternative pooler")
				return err
			}
			
			r.Recorder.Event(cluster, "Normal", "SecretUpdated", 
				fmt.Sprintf("Cluster secrets updated to use pooler %s", alternativePooler.Name))
		} else {
			// No more poolers, revert to cluster service
			contextLogger.Info("No remaining poolers, reverting secrets to cluster service")
			if err := r.SecretUpdater.RevertSecretsToCluster(ctx, cluster); err != nil {
				contextLogger.Error(err, "Failed to revert secrets to cluster service")
				return err
			}
			
			r.Recorder.Event(cluster, "Normal", "SecretReverted", 
				"Cluster secrets reverted to use cluster service")
		}
	} else if activePooler != nil {
		contextLogger.Info("Another pooler is already active, no cleanup needed", "activePooler", activePooler.Name)
	} else {
		// No active poolers, revert to cluster service
		contextLogger.Info("No active poolers found, reverting secrets to cluster service")
		if err := r.SecretUpdater.RevertSecretsToCluster(ctx, cluster); err != nil {
			contextLogger.Error(err, "Failed to revert secrets to cluster service")
			return err
		}
		
		r.Recorder.Event(cluster, "Normal", "SecretReverted", 
			"Cluster secrets reverted to use cluster service")
	}


	return nil
}

// listPoolersForCluster is a helper method to list poolers for a cluster
func (r *PoolerReconciler) listPoolersForCluster(ctx context.Context, clusterName, namespace string) ([]apiv1.Pooler, error) {
	var poolerList apiv1.PoolerList
	listOpts := []client.ListOption{
		client.InNamespace(namespace),
	}

	if err := r.List(ctx, &poolerList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list poolers in namespace %s: %w", namespace, err)
	}

	// Filter poolers that are associated with the specified cluster
	var clusterPoolers []apiv1.Pooler
	for _, pooler := range poolerList.Items {
		if pooler.Spec.Cluster.Name == clusterName {
			clusterPoolers = append(clusterPoolers, pooler)
		}
	}

	return clusterPoolers, nil
}

// handleOrphanedPoolerCleanup handles cleanup when a pooler resource is already deleted
func (r *PoolerReconciler) handleOrphanedPoolerCleanup(ctx context.Context, poolerKey types.NamespacedName) error {
	contextLogger := log.FromContext(ctx).WithValues(
		"pooler", poolerKey.Name,
		"namespace", poolerKey.Namespace,
	)



	// We can't know which cluster this pooler belonged to since it's deleted
	// So we'll check all clusters in the namespace for any that might need cleanup
	var clusters apiv1.ClusterList
	if err := r.List(ctx, &clusters, client.InNamespace(poolerKey.Namespace)); err != nil {
		return fmt.Errorf("failed to list clusters in namespace %s: %w", poolerKey.Namespace, err)
	}

	for _, cluster := range clusters.Items {
		// Check if this cluster has any active poolers
		activePooler, err := r.SecretUpdater.GetActivePoolerForCluster(ctx, cluster.Name, cluster.Namespace)
		if err != nil {
			contextLogger.Error(err, "Failed to check active poolers for cluster", "cluster", cluster.Name)
			continue
		}

		if activePooler == nil {
			// No active poolers for this cluster, check if secrets need to be reverted
			contextLogger.Info("No active poolers found for cluster, checking if secrets need reversion", "cluster", cluster.Name)
			
			// Check if the cluster secrets are currently pointing to a pooler service
			appSecret := &corev1.Secret{}
			secretKey := types.NamespacedName{Name: cluster.GetApplicationSecretName(), Namespace: cluster.Namespace}
			
			if err := r.Get(ctx, secretKey, appSecret); err != nil {
				if !apierrs.IsNotFound(err) {
					contextLogger.Error(err, "Failed to get application secret", "secret", secretKey.Name)
				}
				continue
			}

			// Check if the host in the secret is pointing to a pooler (not the cluster service)
			currentHost := string(appSecret.Data["host"])
			clusterServiceName := cluster.GetServiceReadWriteName()
			
			if currentHost != "" && currentHost != clusterServiceName {
				contextLogger.Info("Secret appears to be pointing to a pooler service, reverting to cluster service", 
					"currentHost", currentHost, "clusterService", clusterServiceName)
				
				if err := r.SecretUpdater.RevertSecretsToCluster(ctx, &cluster); err != nil {
					contextLogger.Error(err, "Failed to revert secrets to cluster service", "cluster", cluster.Name)
					continue
				}
				
				r.Recorder.Event(&cluster, "Normal", "SecretReverted", 
					"Cluster secrets reverted to use cluster service after pooler deletion")
			}
		}
	}

	return nil
}