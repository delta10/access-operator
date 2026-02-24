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
	"time"

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

const privilegeDriftRequeueInterval = 30 * time.Second
const syncedRequeueInterval = 5 * time.Minute
const postgresAccessFinalizer = "access.k8s.delta10.nl/finalizer"

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=postgresaccesses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=postgresaccesses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=postgresaccesses/finalizers,verbs=update

// RBAC permissions for CronJobs, if needed for finalizer cleanup.
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=cronjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=cronjobs/finalizers,verbs=update

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

	var password string
	inSync := true

	finalized, err := r.finalizePostgresAccess(ctx, &pg)
	if err != nil {
		return ctrl.Result{}, err
	}
	// If the resource is finalized, we return early to avoid requeuing.
	// The finalizer will have been removed, so no further reconciliation will occur for this resource.
	if finalized {
		return ctrl.Result{}, nil
	}

	// reconcile the secret that holds the connection details for this PostgresAccess CR.
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, sec, func() error {
		sec.Type = corev1.SecretTypeOpaque

		if sec.Data == nil {
			sec.Data = map[string][]byte{}
		}

		existingPassword, ok := sec.Data["password"]

		if ok && len(existingPassword) > 0 {
			password = string(existingPassword)
		} else {
			password = rand.Text()
			inSync = false
		}

		sec.Data["username"] = []byte(pg.Spec.Username)
		sec.Data["password"] = []byte(password)

		return controllerutil.SetControllerReference(&pg, sec, r.Scheme)
	})
	if err != nil {
		log.Error(err, "failed to create/update secret", "secret", key.String())
		return ctrl.Result{}, err
	}

	pgSync, err := r.reconcilePostgresAccess(ctx, &pg)
	if err != nil {
		log.Error(err, "failed to reconcile PostgresAccess")
		return ctrl.Result{}, err
	}

	if inSync {
		inSync = pgSync
	}

	if inSync {
		return ctrl.Result{RequeueAfter: syncedRequeueInterval}, nil
	}

	return ctrl.Result{RequeueAfter: privilegeDriftRequeueInterval}, nil
}

// reconcilePostgresAccess connects to the PostgreSQL database, retrieves current grants and users.
// Then ensures that the grants specified in the PostgresAccess CR are correctly applied in the database.
// It also creates/removes any users as needed.
func (r *PostgresAccessReconciler) reconcilePostgresAccess(ctx context.Context, pg *accessv1.PostgresAccess) (bool, error) {
	log := logf.FromContext(ctx)

	connectionString, err := r.getConnectionString(ctx, pg)
	if err != nil {
		log.Error(err, "failed to get connection string, no valid connection details provided")
		return false, err
	}

	if r.DB == nil {
		r.DB = NewPostgresDB()
	}

	err = r.DB.Connect(ctx, connectionString)
	if err != nil {
		log.Error(err, "Unable to connect to database", "connectionString", connectionString)
		return false, err
	}
	defer func() {
		if err := r.DB.Close(ctx); err != nil {
			log.Error(err, "failed to close database connection")
		}
	}()

	grants, err := r.DB.GetGrants(ctx)
	if err != nil {
		log.Error(err, "failed to get grants from PostgreSQL")
		return false, err
	}

	users, err := r.DB.GetUsers(ctx)
	if err != nil {
		return false, err
	}

	configs, err := getAllPostgresAccessGrantsAndUsers(ctx, r.Client, pg.Namespace)
	if err != nil {
		return false, err
	}

	usersHandled := make(map[string]bool)
	inSync := true

	for _, config := range configs {
		password, err := getUserPassword(ctx, r.Client, pg.Namespace, config.GeneratedSecret)
		if err != nil {
			log.Error(err, "failed to get user password from generated secret", "username", config.Username, "secret", config.GeneratedSecret)
			continue
		}

		// check if the user exists in the database, if not create it
		if !slices.Contains(users, config.Username) {
			err = r.DB.CreateUser(ctx, config.Username, password)
			if err != nil {
				log.Error(err, "failed to create user in PostgreSQL", "username", config.Username)
				continue
			}
			inSync = false
		} else {
			// Update the password for existing user to ensure it matches the generated secret
			// we skip the check if the password is already correct because that would make more queries and create more complexity.
			err = r.DB.UpdateUserPassword(ctx, config.Username, password)
			if err != nil {
				log.Error(err, "failed to update user password in PostgreSQL for existing user", "username", config.Username)
				continue
			}

			// don't check sync here to avoid extra queries to the database, we'll trust it's fine
		}

		toGrant, toRevoke := diffGrants(grants[config.Username], config.Grants)
		if len(toGrant) > 0 || len(toRevoke) > 0 {
			inSync = false
		}

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

		usersHandled[config.Username] = true
	}

	// remove users that are not handled by any PostgresAccess CR anymore
	// Use Restrict policy as the safe default for orphaned users
	for _, user := range users {
		if !usersHandled[user] {
			err = r.DB.DropUser(ctx, user, accessv1.CleanupPolicyRestrict)
			if err != nil {
				log.Error(err, "failed to drop user in PostgreSQL", "username", user)
				continue
			}
		}
	}

	return inSync, nil
}

func (r *PostgresAccessReconciler) finalizePostgresAccess(ctx context.Context, pg *accessv1.PostgresAccess) (bool, error) {
	log := logf.FromContext(ctx)
	if pg.DeletionTimestamp.IsZero() {
		// The object is not being deleted, so if it does not have our finalizer,
		// then let's add the finalizer and update the object. This is equivalent
		// to registering our finalizer.
		if !controllerutil.ContainsFinalizer(pg, postgresAccessFinalizer) {
			controllerutil.AddFinalizer(pg, postgresAccessFinalizer)
			if err := r.Update(ctx, pg); err != nil {
				return false, err
			}
		}
		return false, nil
	}

	if !controllerutil.ContainsFinalizer(pg, postgresAccessFinalizer) {
		return true, nil
	}

	connectionString, err := r.getConnectionString(ctx, pg)
	if err != nil {
		log.Error(err, "failed to get connection string during finalization")
		return true, err
	}

	if r.DB == nil {
		r.DB = NewPostgresDB()
	}

	err = r.DB.Connect(ctx, connectionString)
	if err != nil {
		log.Error(err, "Unable to connect to database during finalization")
		return true, err
	}
	defer func() {
		if err := r.DB.Close(ctx); err != nil {
			log.Error(err, "failed to close database connection during finalization")
		}
	}()

	users, err := r.DB.GetUsers(ctx)
	if err != nil {
		log.Error(err, "failed to get users from PostgreSQL during finalization")
		return true, err
	}

	// Determine cleanup policy, default to Restrict if not specified
	cleanupPolicy := accessv1.CleanupPolicyRestrict
	if pg.Spec.CleanupPolicy != nil {
		cleanupPolicy = *pg.Spec.CleanupPolicy
	}

	for _, user := range users {
		if pg.Spec.Username == user {
			err = r.DB.DropUser(ctx, user, cleanupPolicy)
			if err != nil {
				log.Error(err, "failed to drop user in PostgreSQL during finalization", "username", user)
				continue
			}
		}
	}

	controllerutil.RemoveFinalizer(pg, postgresAccessFinalizer)
	if err := r.Update(ctx, pg); err != nil {
		return true, err
	}

	return true, nil
}

// getConnectionString constructs the PostgreSQL connection string based on the PostgresAccess spec.
// It supports both direct connection details and referencing an existing secret for connection information.
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
	if c.Username != nil && c.Password != nil &&
		c.Host != nil && *c.Host != "" && c.Port != nil && c.Database != nil && *c.Database != "" {

		sslMode := "require" // secure default
		if c.SSLMode != nil && *c.SSLMode != "" {
			sslMode = *c.SSLMode
		}

		username, err := r.resolveValueOrSecretRef(ctx, c.Username, pg.Namespace)
		if err != nil {
			return "", fmt.Errorf("failed to resolve username: %w", err)
		}

		password, err := r.resolveValueOrSecretRef(ctx, c.Password, pg.Namespace)
		if err != nil {
			return "", fmt.Errorf("failed to resolve password: %w", err)
		}

		return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=%s",
			username, password, *c.Host, *c.Port, *c.Database, sslMode), nil
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

// resolveValueOrSecretRef resolves a value that can be either a direct value or a secret reference
func (r *PostgresAccessReconciler) resolveValueOrSecretRef(ctx context.Context, ref *accessv1.SecretKeySelector, namespace string) (string, error) {
	if ref == nil {
		return "", fmt.Errorf("value or secret reference is nil")
	}

	if ref.Value != nil {
		return *ref.Value, nil
	}

	if ref.SecretRef != nil {
		var sec corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Name: ref.SecretRef.Name, Namespace: namespace}, &sec); err != nil {
			return "", fmt.Errorf("failed to get secret: %w", err)
		}
		data, ok := sec.Data[ref.SecretRef.Key]
		if !ok || len(data) == 0 {
			return "", fmt.Errorf("secret is missing key: %s", ref.SecretRef.Key)
		}
		return string(data), nil
	}

	return "", fmt.Errorf("neither value nor secretRef is specified")
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

	existingDatabase, ok := existingSec.Data["dbname"]
	if !ok || len(existingDatabase) == 0 {
		return ConnectionDetails{}, fmt.Errorf("existing secret is missing database")
	}

	// optionally allow "database" as an alternative key for the database name
	if !ok {
		existingDatabase, ok = existingSec.Data["database"]
		if !ok || len(existingDatabase) == 0 {
			return ConnectionDetails{}, fmt.Errorf("existing secret is missing database (dbname or database key)")
		}
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

// UserGrants represents a username and their associated grants
type UserGrants struct {
	Username        string
	GeneratedSecret string
	Grants          []accessv1.GrantSpec
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

	result := make([]UserGrants, 0, len(pgList.Items))
	for _, pg := range pgList.Items {
		// Add the username and grants to the result
		result = append(result, UserGrants{
			Username:        pg.Spec.Username,
			GeneratedSecret: pg.Spec.GeneratedSecret,
			Grants:          pg.Spec.Grants,
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

func getUserPassword(ctx context.Context, c client.Client, namespace, secretName string) (string, error) {
	if secretName == "" {
		return "", fmt.Errorf("generatedSecret is not specified in PostgresAccess spec")
	}

	var userSec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, &userSec); err != nil {
		return "", fmt.Errorf("failed to get user secret for password retrieval: %w", err)
	}

	existingPassword, ok := userSec.Data["password"]
	if !ok || len(existingPassword) == 0 {
		return "", fmt.Errorf("user secret is missing password")
	}

	return string(existingPassword), nil
}
