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

package registry

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cloudnative-pg/machinery/pkg/log"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
)

// PoolerRegistry defines the interface for managing pooler registrations and precedence
type PoolerRegistry interface {
	// RegisterPooler adds a pooler to the registry
	RegisterPooler(ctx context.Context, pooler *apiv1.Pooler) error

	// UnregisterPooler removes a pooler from the registry
	UnregisterPooler(ctx context.Context, poolerName, namespace string) error

	// GetActivePooler returns the most recent pooler for a cluster
	GetActivePooler(ctx context.Context, clusterName, namespace string) (*apiv1.Pooler, error)

	// ListPoolersForCluster returns all poolers for a cluster
	ListPoolersForCluster(ctx context.Context, clusterName, namespace string) ([]apiv1.Pooler, error)
}

// poolerRegistry implements the PoolerRegistry interface using Kubernetes API
type poolerRegistry struct {
	client.Client
}

// NewPoolerRegistry creates a new PoolerRegistry instance
func NewPoolerRegistry(client client.Client) PoolerRegistry {
	return &poolerRegistry{
		Client: client,
	}
}

// RegisterPooler adds a pooler to the registry (no-op since we use Kubernetes API directly)
func (r *poolerRegistry) RegisterPooler(ctx context.Context, pooler *apiv1.Pooler) error {
	contextLogger := log.FromContext(ctx).WithValues(
		"pooler", pooler.Name,
		"cluster", pooler.Spec.Cluster.Name,
		"namespace", pooler.Namespace,
	)

	contextLogger.Debug("Registering pooler in registry")
	// Since we use Kubernetes API directly, registration is implicit when the pooler exists
	return nil
}

// UnregisterPooler removes a pooler from the registry (no-op since we use Kubernetes API directly)
func (r *poolerRegistry) UnregisterPooler(ctx context.Context, poolerName, namespace string) error {
	contextLogger := log.FromContext(ctx).WithValues(
		"pooler", poolerName,
		"namespace", namespace,
	)

	contextLogger.Debug("Unregistering pooler from registry")
	// Since we use Kubernetes API directly, unregistration is implicit when the pooler is deleted
	return nil
}

// GetActivePooler returns the most recent pooler for a cluster based on creation timestamp
func (r *poolerRegistry) GetActivePooler(ctx context.Context, clusterName, namespace string) (*apiv1.Pooler, error) {
	contextLogger := log.FromContext(ctx).WithValues(
		"cluster", clusterName,
		"namespace", namespace,
	)

	poolers, err := r.ListPoolersForCluster(ctx, clusterName, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to list poolers for cluster %s: %w", clusterName, err)
	}

	if len(poolers) == 0 {
		contextLogger.Debug("No poolers found for cluster")
		return nil, nil
	}

	// Sort poolers by creation timestamp (most recent first), then by name for tiebreaker
	r.sortPoolersByPrecedence(poolers)

	activePooler := &poolers[0]
	contextLogger.Debug("Found active pooler", "activePooler", activePooler.Name)

	return activePooler, nil
}

// ListPoolersForCluster returns all poolers associated with a specific cluster
func (r *poolerRegistry) ListPoolersForCluster(ctx context.Context, clusterName, namespace string) ([]apiv1.Pooler, error) {
	contextLogger := log.FromContext(ctx).WithValues(
		"cluster", clusterName,
		"namespace", namespace,
	)

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

	contextLogger.Debug("Found poolers for cluster", "count", len(clusterPoolers))
	return clusterPoolers, nil
}

// sortPoolersByPrecedence sorts poolers by creation timestamp (most recent first)
// with lexicographic name ordering as tiebreaker
func (r *poolerRegistry) sortPoolersByPrecedence(poolers []apiv1.Pooler) {
	sort.Slice(poolers, func(i, j int) bool {
		// Compare creation timestamps
		timeI := poolers[i].CreationTimestamp
		timeJ := poolers[j].CreationTimestamp

		// If timestamps are different, sort by most recent first
		if !timeI.Equal(&timeJ) {
			return timeI.After(timeJ.Time)
		}

		// If timestamps are equal, use lexicographic ordering of names as tiebreaker
		return strings.Compare(poolers[i].Name, poolers[j].Name) < 0
	})
}

// GetPoolerByName retrieves a specific pooler by name and namespace
func (r *poolerRegistry) GetPoolerByName(ctx context.Context, poolerName, namespace string) (*apiv1.Pooler, error) {
	pooler := &apiv1.Pooler{}
	poolerKey := client.ObjectKey{Name: poolerName, Namespace: namespace}

	if err := r.Get(ctx, poolerKey, pooler); err != nil {
		if apierrs.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get pooler %s: %w", poolerName, err)
	}

	return pooler, nil
}