/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"crypto/rand"

	accessv1 "github.com/delta10/access-operator/api/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// PostgresAccessReconciler reconciles a PostgresAccess object
type PostgresAccessReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	DB     DBInterface
}

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=postgresaccesses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=postgresaccesses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=postgresaccesses/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *PostgresAccessReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	log.Info("Reconciling PostgresAccess", "namespace", req.Namespace, "name", req.Name)

	var pg accessv1.PostgresAccess
	if err := r.Get(ctx, req.NamespacedName, &pg); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	key := types.NamespacedName{
		Name:      pg.Spec.GeneratedSecret,
		Namespace: req.Namespace,
	}

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
	}

	var username string
	var password string

	if pg.Spec.Username != nil && *pg.Spec.Username != "" {
		username = *pg.Spec.Username
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sec, func() error {
		sec.Type = corev1.SecretTypeOpaque

		if sec.Data == nil {
			sec.Data = map[string][]byte{}
		}

		existingPassword, ok := sec.Data["password"]

		if ok && len(existingPassword) > 0 {
			password = string(existingPassword)
		} else {
			password = rand.Text()
		}

		sec.Data["username"] = []byte(username)
		sec.Data["password"] = []byte(password)

		return controllerutil.SetControllerReference(&pg, sec, r.Scheme)
	})
	if err != nil {
		log.Error(err, "failed to create/update secret", "secret", key.String())
		return ctrl.Result{}, err
	}

	connectionString, err := r.getConnectionString(ctx, &pg)
	if err != nil {
		log.Error(err, "failed to get connection string, no valid connection details provided")
		return ctrl.Result{}, err
	}
	if r.DB == nil {
		r.DB = NewPostgresDB()
	}

	err = r.DB.Connect(ctx, connectionString)
	if err != nil {
		log.Error(err, "Unable to connect to database", "connectionString", connectionString)
		return ctrl.Result{}, err
	}
	defer func() {
		if err := r.DB.Close(ctx); err != nil {
			log.Error(err, "failed to close database connection")
		}
	}()

	grants, err := r.DB.GetGrants(ctx)
	if err != nil {
		log.Error(err, "failed to get grants from PostgreSQL")
		return ctrl.Result{}, err
	}

	users, err := r.DB.GetUsers(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	configs, err := getAllPostgresAccessGrantsAndUsers(ctx, r.Client, pg.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	for _, config := range configs {
		// check if the user exists in the database, if not create it
		if !slices.Contains(users, config.Username) {
			err = r.DB.CreateUser(ctx, config.Username, password)
			if err != nil {
				log.Error(err, "failed to create user in PostgreSQL", "username", config.Username)
				continue
			}
		}

		toGrant, toRevoke := diffGrants(grants[config.Username], config.Grants)

		err = r.DB.GrantPrivileges(ctx, toGrant, config.Username)
		if err != nil {
			log.Error(err, "failed to grant privileges in PostgreSQL", "username", config.Username)
			continue
		}

		err = r.DB.RevokePrivileges(ctx, toRevoke, config.Username)
		if err != nil {
			log.Error(err, "failed to revoke privileges in PostgreSQL", "username", config.Username)
			continue
		}
	}

	return ctrl.Result{}, nil
}

func (r *PostgresAccessReconciler) getConnectionString(ctx context.Context, pg *accessv1.PostgresAccess) (string, error) {
	if pg.Spec.Connection.ExistingSecret != nil && *pg.Spec.Connection.ExistingSecret != "" {
		connection, err := getExistingSecretConnectionDetails(ctx, r.Client, *pg.Spec.Connection.ExistingSecret, pg.Namespace)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=%s",
			connection.Username, connection.Password, connection.Host, connection.Port, connection.Database, connection.SSLMode), nil
	}

	c := pg.Spec.Connection
	if c.Username != nil && c.Username.Value != nil && c.Password != nil && c.Password.Value != nil &&
		c.Host != nil && *c.Host != "" && c.Port != nil && c.Database != nil && *c.Database != "" {

		sslMode := "require" // secure default
		if c.SSLMode != nil && *c.SSLMode != "" {
			sslMode = *c.SSLMode
		}

		return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=%s",
			*c.Username.Value, *c.Password.Value, *c.Host, *c.Port, *c.Database, sslMode), nil
	}

	return "", fmt.Errorf("no valid connection details provided")
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresAccessReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&accessv1.PostgresAccess{}).
		Named("postgresaccess").
		Owns(&corev1.Secret{}).
		Complete(r)
}

func getExistingSecretConnectionDetails(ctx context.Context, c client.Client, secretName, namespace string) (ConnectionDetails, error) {
	var existingSec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, &existingSec); err != nil {
		return ConnectionDetails{}, fmt.Errorf("failed to get existing secret for connection details: %w", err)
	}

	existingUsername, ok := existingSec.Data["username"]
	if !ok || len(existingUsername) == 0 {
		return ConnectionDetails{}, fmt.Errorf("existing secret is missing username")
	}

	existingPassword, ok := existingSec.Data["password"]
	if !ok || len(existingPassword) == 0 {
		return ConnectionDetails{}, fmt.Errorf("existing secret is missing password")
	}

	existingHost, ok := existingSec.Data["host"]
	if !ok || len(existingHost) == 0 {
		return ConnectionDetails{}, fmt.Errorf("existing secret is missing host")
	}

	existingPort, ok := existingSec.Data["port"]
	if !ok || len(existingPort) == 0 {
		return ConnectionDetails{}, fmt.Errorf("existing secret is missing port")
	}

	existingDatabase, ok := existingSec.Data["database"]
	if !ok || len(existingDatabase) == 0 {
		return ConnectionDetails{}, fmt.Errorf("existing secret is missing database")
	}

	// sslmode is optional, defaults to "require" for security
	sslMode := "require"
	if existingSSLMode, ok := existingSec.Data["sslmode"]; ok && len(existingSSLMode) > 0 {
		sslMode = string(existingSSLMode)
	}

	return ConnectionDetails{
		Username: string(existingUsername),
		Password: string(existingPassword),
		Host:     string(existingHost),
		Port:     string(existingPort),
		Database: string(existingDatabase),
		SSLMode:  sslMode,
	}, nil
}

/*
func getControlledSecrets(ctx context.Context, c client.Client, pg *accessv1.PostgresAccess) ([]corev1.Secret, error) {
	var secretList corev1.SecretList
	if err := c.List(ctx, &secretList, client.InNamespace(pg.Namespace), client.MatchingFields{".metadata.controller": pg.Name}); err != nil {
		return nil, fmt.Errorf("failed to list secrets: %w", err)
	}

	var controlledSecrets []corev1.Secret
	for _, sec := range secretList.Items {
		if metav1.IsControlledBy(&sec, pg) {
			controlledSecrets = append(controlledSecrets, sec)
		}
	}
	return controlledSecrets, nil
}
*/

// UserGrants represents a username and their associated grants
type UserGrants struct {
	Username string
	Grants   []accessv1.GrantSpec
}

// getAllPostgresAccessGrantsAndUsers retrieves all PostgresAccess CRs and extracts usernames with their grants
// Returns a slice of UserGrants containing the username and associated grants for each CR
func getAllPostgresAccessGrantsAndUsers(ctx context.Context, c client.Client, namespace string) ([]UserGrants, error) {
	var pgList accessv1.PostgresAccessList

	// List all PostgresAccess resources in the namespace
	// For all namespaces, omit the InNamespace option
	if err := c.List(ctx, &pgList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("failed to list PostgresAccess resources: %w", err)
	}

	var result []UserGrants
	for _, pg := range pgList.Items {
		// Get username from the spec
		username := ""
		if pg.Spec.Username != nil && *pg.Spec.Username != "" {
			username = *pg.Spec.Username
		}

		// Add the username and grants to the result
		result = append(result, UserGrants{
			Username: username,
			Grants:   pg.Spec.Grants,
		})
	}

	return result, nil
}

// diffGrants compares the current grants with the desired grants and determines which grants need to be added or revoked
// When current is nil or empty, all desired grants will be returned in toGrant and toRevoke will be empty
func diffGrants(current, desired []accessv1.GrantSpec) (toGrant, toRevoke []accessv1.GrantSpec) {
	// Create maps for easy lookup
	currentMap := make(map[string]accessv1.GrantSpec)
	desiredMap := make(map[string]accessv1.GrantSpec)

	for _, grant := range current {
		for _, privilege := range grant.Privileges {
			key := fmt.Sprintf("%s:%s", grant.Database, privilege)
			currentMap[key] = accessv1.GrantSpec{
				Database:   grant.Database,
				Schema:     grant.Schema,
				Privileges: []string{privilege},
			}
		}
	}

	for _, grant := range desired {
		for _, privilege := range grant.Privileges {
			key := fmt.Sprintf("%s:%s", grant.Database, privilege)
			desiredMap[key] = accessv1.GrantSpec{
				Database:   grant.Database,
				Schema:     grant.Schema,
				Privileges: []string{privilege},
			}
		}
	}

	// Determine grants to add
	for key, grant := range desiredMap {
		if _, exists := currentMap[key]; !exists {
			toGrant = append(toGrant, grant)
		}
	}

	// Determine grants to revoke
	for key, grant := range currentMap {
		if _, exists := desiredMap[key]; !exists {
			toRevoke = append(toRevoke, grant)
		}
	}

	return toGrant, toRevoke
}
