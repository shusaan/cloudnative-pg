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

package secrets

import (
	"context"
	"fmt"
	"net/url"

	"github.com/cloudnative-pg/machinery/pkg/log"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/internal/configuration"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/pooler/registry"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/postgres"
)

// SecretUpdater defines the interface for updating cluster secrets when pooler state changes
type SecretUpdater interface {
	// UpdateSecretsForPooler updates cluster secrets to use pooler service
	UpdateSecretsForPooler(ctx context.Context, cluster *apiv1.Cluster, pooler *apiv1.Pooler) error

	// RevertSecretsToCluster updates cluster secrets to use cluster service
	RevertSecretsToCluster(ctx context.Context, cluster *apiv1.Cluster) error

	// GetActivePoolerForCluster returns the currently active pooler for a cluster
	GetActivePoolerForCluster(ctx context.Context, clusterName, namespace string) (*apiv1.Pooler, error)
}

// secretUpdater implements the SecretUpdater interface
type secretUpdater struct {
	client.Client
	registry registry.PoolerRegistry
}

// NewSecretUpdater creates a new SecretUpdater instance
func NewSecretUpdater(client client.Client, registry registry.PoolerRegistry) SecretUpdater {
	return &secretUpdater{
		Client:   client,
		registry: registry,
	}
}

// UpdateSecretsForPooler updates cluster secrets to use pooler service address
func (s *secretUpdater) UpdateSecretsForPooler(ctx context.Context, cluster *apiv1.Cluster, pooler *apiv1.Pooler) error {
	contextLogger := log.FromContext(ctx).WithValues(
		"cluster", cluster.Name,
		"pooler", pooler.Name,
		"namespace", cluster.Namespace,
	)



	// Get pooler service name
	poolerServiceName := pooler.Name
	
	// Update application secret
	appSecretName := cluster.GetApplicationSecretName()
	if err := s.updateSecretForService(ctx, appSecretName, cluster.Namespace, poolerServiceName, cluster.GetApplicationDatabaseName()); err != nil {
		contextLogger.Error(err, "Failed to update application secret for pooler")
		return fmt.Errorf("failed to update application secret: %w", err)
	}

	// Update superuser secret if enabled
	if cluster.GetEnableSuperuserAccess() {
		superuserSecretName := cluster.GetSuperuserSecretName()
		if err := s.updateSecretForService(ctx, superuserSecretName, cluster.Namespace, poolerServiceName, "postgres"); err != nil {
			contextLogger.Error(err, "Failed to update superuser secret for pooler")
			return fmt.Errorf("failed to update superuser secret: %w", err)
		}
	}

	contextLogger.Info("Successfully updated cluster secrets to use pooler service", "pooler", pooler.Name)
	return nil
}

// RevertSecretsToCluster updates cluster secrets to use cluster service address
func (s *secretUpdater) RevertSecretsToCluster(ctx context.Context, cluster *apiv1.Cluster) error {
	contextLogger := log.FromContext(ctx).WithValues(
		"cluster", cluster.Name,
		"namespace", cluster.Namespace,
	)

	contextLogger.Info("Reverting cluster secrets to use cluster service")

	// Get cluster service name (read-write service)
	clusterServiceName := cluster.GetServiceReadWriteName()

	// Update application secret
	if err := s.updateSecretForService(ctx, cluster.GetApplicationSecretName(), cluster.Namespace, clusterServiceName, cluster.GetApplicationDatabaseName()); err != nil {
		contextLogger.Error(err, "Failed to revert application secret to cluster service")
		return fmt.Errorf("failed to revert application secret: %w", err)
	}

	// Update superuser secret if enabled
	if cluster.GetEnableSuperuserAccess() {
		if err := s.updateSecretForService(ctx, cluster.GetSuperuserSecretName(), cluster.Namespace, clusterServiceName, "postgres"); err != nil {
			contextLogger.Error(err, "Failed to revert superuser secret to cluster service")
			return fmt.Errorf("failed to revert superuser secret: %w", err)
		}
	}

	contextLogger.Info("Successfully reverted cluster secrets to use cluster service")
	return nil
}

// GetActivePoolerForCluster returns the currently active pooler for a cluster
func (s *secretUpdater) GetActivePoolerForCluster(ctx context.Context, clusterName, namespace string) (*apiv1.Pooler, error) {
	return s.registry.GetActivePooler(ctx, clusterName, namespace)
}

// updateSecretForService updates a secret to use the specified service address
func (s *secretUpdater) updateSecretForService(ctx context.Context, secretName, namespace, serviceName, dbname string) error {
	contextLogger := log.FromContext(ctx).WithValues(
		"secret", secretName,
		"service", serviceName,
		"namespace", namespace,
	)

	// Get the existing secret
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{Name: secretName, Namespace: namespace}
	
	if err := s.Get(ctx, secretKey, secret); err != nil {
		if apierrs.IsNotFound(err) {
			contextLogger.Info("Secret not found, skipping update")
			return nil
		}
		return fmt.Errorf("failed to get secret %s: %w", secretName, err)
	}

	// Validate that the target service exists
	service := &corev1.Service{}
	serviceKey := types.NamespacedName{Name: serviceName, Namespace: namespace}
	if err := s.Get(ctx, serviceKey, service); err != nil {
		if apierrs.IsNotFound(err) {
			contextLogger.Warning("Target service not found, skipping secret update", "service", serviceName)
			return fmt.Errorf("target service %s not found", serviceName)
		}
		return fmt.Errorf("failed to validate service %s: %w", serviceName, err)
	}

	// Update connection-related fields while preserving others
	if err := s.updateConnectionFields(secret, serviceName, namespace, dbname); err != nil {
		return fmt.Errorf("failed to update connection fields: %w", err)
	}

	// Update the secret with conflict resolution
	if err := s.Update(ctx, secret); err != nil {
		if apierrs.IsConflict(err) {
			contextLogger.Info("Conflict detected while updating secret, will be retried by controller")
			return fmt.Errorf("conflict updating secret %s (will be retried): %w", secretName, err)
		}
		return fmt.Errorf("failed to update secret %s: %w", secretName, err)
	}

	contextLogger.Info("Successfully updated secret connection fields")
	return nil
}

// updateConnectionFields updates the connection-related fields in a secret
func (s *secretUpdater) updateConnectionFields(secret *corev1.Secret, hostname, namespace, dbname string) error {
	if secret.Data == nil {
		secret.Data = make(map[string][]byte)
	}

	// Get existing values for username and password
	username := string(secret.Data["username"])
	password := string(secret.Data["password"])
	
	if username == "" {
		username = string(secret.Data["user"])
	}

	// Use provided dbname or fall back to existing
	if dbname == "" {
		dbname = string(secret.Data["dbname"])
	}

	// Build connection strings
	hostWithNamespace := fmt.Sprintf("%s.%s:%d", hostname, namespace, postgres.ServerPort)
	hostWithFQDN := fmt.Sprintf(
		"%s.%s.svc.%s:%d",
		hostname,
		namespace,
		configuration.Current.KubernetesClusterDomain,
		postgres.ServerPort,
	)

	// Update connection-related fields
	secret.Data["host"] = []byte(hostname)
	secret.Data["port"] = []byte(fmt.Sprintf("%d", postgres.ServerPort))
	
	if dbname != "" {
		secret.Data["dbname"] = []byte(dbname)
	}

	// Build and update URI fields
	if username != "" && password != "" {
		namespacedBuilder := &connectionStringBuilder{
			host:     hostWithNamespace,
			dbname:   dbname,
			username: username,
			password: password,
		}

		fqdnBuilder := &connectionStringBuilder{
			host:     hostWithFQDN,
			dbname:   dbname,
			username: username,
			password: password,
		}

		secret.Data["uri"] = []byte(namespacedBuilder.buildPostgres())
		secret.Data["jdbc-uri"] = []byte(namespacedBuilder.buildJdbc())
		secret.Data["fqdn-uri"] = []byte(fqdnBuilder.buildPostgres())
		secret.Data["fqdn-jdbc-uri"] = []byte(fqdnBuilder.buildJdbc())

		// Update pgpass entry
		pgpass := fmt.Sprintf(
			"%v:%v:%v:%v:%v\n",
			hostname,
			postgres.ServerPort,
			dbname,
			username,
			password,
		)
		secret.Data["pgpass"] = []byte(pgpass)
	}

	return nil
}

// connectionStringBuilder helps build PostgreSQL connection strings
type connectionStringBuilder struct {
	host     string
	dbname   string
	username string
	password string
}

func (c connectionStringBuilder) buildPostgres() string {
	postgresURI := url.URL{
		Scheme: "postgresql",
		User:   url.UserPassword(c.username, c.password),
		Host:   c.host,
		Path:   c.dbname,
	}

	return postgresURI.String()
}

func (c connectionStringBuilder) buildJdbc() string {
	jdbcURI := &url.URL{
		Scheme: "jdbc:postgresql",
		Host:   c.host,
		Path:   c.dbname,
	}
	q := jdbcURI.Query()
	q.Set("user", c.username)
	q.Set("password", c.password)
	jdbcURI.RawQuery = q.Encode()
	return jdbcURI.String()
}