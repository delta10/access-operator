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
	"errors"
	"fmt"
	"net"
	neturl "net/url"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
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
	Scheme   *runtime.Scheme
	DB       DBInterface
	Recorder events.EventRecorder
}

const privilegeDriftRequeueInterval = 30 * time.Second
const syncedRequeueInterval = 5 * time.Minute
const postgresAccessFinalizer = "access.k8s.delta10.nl/finalizer"
const (
	postgresAccessReadyConditionType      = "Ready"
	postgresAccessSuccessConditionType    = "Success"
	postgresAccessInProgressConditionType = "InProgress"
)

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=postgresaccesses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=postgresaccesses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=postgresaccesses/finalizers,verbs=update
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=controllers,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

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
		return r.returnWithErrorStatus(ctx, &pg, "FinalizeFailed", err)
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
		return r.returnWithErrorStatus(ctx, &pg, "SecretSyncFailed", err)
	}

	pgSync, err := r.reconcilePostgresAccess(ctx, &pg)
	if err != nil {
		log.Error(err, "failed to reconcile PostgresAccess")
		return r.returnWithErrorStatus(ctx, &pg, "DatabaseSyncFailed", err)
	}

	if inSync {
		inSync = pgSync
	}

	if inSync {
		if err := r.setReconcileStatus(
			ctx,
			req.NamespacedName,
			accessv1.ReconcileStateSuccess,
			"Ready",
			"PostgresAccess is in sync",
		); err != nil {
			return ctrl.Result{}, err
		}

		if &pg != nil {
			r.emitEvent(&pg, corev1.EventTypeNormal, "ReconcileSuccess", "PostgresAccess is in sync and ready")
		}

		return ctrl.Result{RequeueAfter: syncedRequeueInterval}, nil
	}

	if err := r.setReconcileStatus(
		ctx,
		req.NamespacedName,
		accessv1.ReconcileStateInProgress,
		"Reconciling",
		"PostgresAccess is not yet in sync",
	); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: privilegeDriftRequeueInterval}, nil
}

func (r *PostgresAccessReconciler) returnWithErrorStatus(
	ctx context.Context,
	pg *accessv1.PostgresAccess,
	reason string,
	reconcileErr error,
) (ctrl.Result, error) {
	if pg != nil {
		r.emitEvent(pg, corev1.EventTypeWarning, reason, reconcileErr.Error())
	}

	key := types.NamespacedName{}
	if pg != nil {
		key = types.NamespacedName{Name: pg.Name, Namespace: pg.Namespace}
	}

	if statusErr := r.setReconcileStatus(
		ctx,
		key,
		accessv1.ReconcileStateError,
		reason,
		reconcileErr.Error(),
	); statusErr != nil {
		return ctrl.Result{}, errors.Join(reconcileErr, fmt.Errorf("failed to update status condition: %w", statusErr))
	}

	return ctrl.Result{}, reconcileErr
}

func (r *PostgresAccessReconciler) setReconcileStatus(
	ctx context.Context,
	key types.NamespacedName,
	reconcileState accessv1.ReconcileState,
	reason, message string,
) error {
	if key.Name == "" || key.Namespace == "" {
		return nil
	}

	var latest accessv1.PostgresAccess
	if err := r.Get(ctx, key, &latest); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	readyStatus := metav1.ConditionFalse
	successStatus := metav1.ConditionFalse
	inProgressStatus := metav1.ConditionFalse

	switch reconcileState {
	case accessv1.ReconcileStateSuccess:
		readyStatus = metav1.ConditionTrue
		successStatus = metav1.ConditionTrue
	case accessv1.ReconcileStateInProgress:
		inProgressStatus = metav1.ConditionTrue
	case accessv1.ReconcileStateError:
	default:
		return fmt.Errorf("invalid reconcile state %q", reconcileState)
	}

	meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
		Type:               postgresAccessReadyConditionType,
		Status:             readyStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: latest.GetGeneration(),
	})
	meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
		Type:               postgresAccessSuccessConditionType,
		Status:             successStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: latest.GetGeneration(),
	})
	meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
		Type:               postgresAccessInProgressConditionType,
		Status:             inProgressStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: latest.GetGeneration(),
	})
	latest.Status.LastLog = message
	latest.Status.LastReconcileState = reconcileState

	return r.Status().Update(ctx, &latest)
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
		// Mark CR-managed users up front so cleanup never drops them due a transient reconciliation error.
		usersHandled[config.Username] = true

		password, err := getUserPassword(ctx, r.Client, pg.Namespace, config.GeneratedSecret)
		if err != nil {
			log.Error(err, "failed to get user password from generated secret", "username", config.Username, "secret", config.GeneratedSecret)
			inSync = false
			continue
		}

		// check if the user exists in the database, if not create it
		if !slices.Contains(users, config.Username) {
			err = r.DB.CreateUser(ctx, config.Username, password)
			if err != nil {
				log.Error(err, "failed to create user in PostgreSQL", "username", config.Username)
				inSync = false
				continue
			}
			inSync = false
		} else {
			// Update the password for existing user to ensure it matches the generated secret
			// we skip the check if the password is already correct because that would make more queries and create more complexity.
			err = r.DB.UpdateUserPassword(ctx, config.Username, password)
			if err != nil {
				log.Error(err, "failed to update user password in PostgreSQL for existing user", "username", config.Username)
				inSync = false
				continue
			}

			// don't check sync here to avoid extra queries to the database, we'll trust it's fine
		}

		toGrant, toRevoke := diffGrants(grants[config.Username], config.Grants)

		err = r.DB.GrantPrivileges(ctx, toGrant, config.Username)
		if err != nil {
			log.Error(err, "failed to grant privileges in PostgreSQL", "username", config.Username)
			inSync = false
			continue
		}

		err = r.DB.RevokePrivileges(ctx, toRevoke, config.Username)
		if err != nil {
			log.Error(err, "failed to revoke privileges in PostgreSQL", "username", config.Username)
			inSync = false
			continue
		}
	}

	// remove users that are not handled by any PostgresAccess CR anymore
	// Use Restrict policy as the safe default for orphaned users
	for _, user := range users {
		if !usersHandled[user] {
			err = r.DB.DropUser(ctx, user, accessv1.CleanupPolicyRestrict)
			if err != nil {
				log.Error(err, "failed to drop user in PostgreSQL", "username", user)
				inSync = false
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
		secretNamespace, err := r.resolveExistingSecretNamespace(ctx, pg)
		if err != nil {
			return "", err
		}

		connection, err := getExistingSecretConnectionDetails(ctx, r.Client, *pg.Spec.Connection.ExistingSecret, secretNamespace, pg)
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

func (r *PostgresAccessReconciler) resolveExistingSecretNamespace(ctx context.Context, pg *accessv1.PostgresAccess) (string, error) {
	secretNamespace := pg.Namespace
	if pg.Spec.Connection.ExistingSecretNamespace == nil {
		return secretNamespace, nil
	}

	requestedNamespace := strings.TrimSpace(*pg.Spec.Connection.ExistingSecretNamespace)
	if requestedNamespace == "" {
		return secretNamespace, nil
	}

	if requestedNamespace == pg.Namespace {
		return requestedNamespace, nil
	}

	allowed, err := r.resolveExistingSecretNamespacePolicy(ctx)
	if err != nil {
		r.emitEvent(pg, corev1.EventTypeWarning, multipleControllersFoundReason, err.Error())
		return "", err
	}
	if !allowed {
		return "", fmt.Errorf(
			"cross-namespace connection secret references are disabled: requested namespace %q from PostgresAccess namespace %q",
			requestedNamespace, pg.Namespace,
		)
	}

	return requestedNamespace, nil
}

func (r *PostgresAccessReconciler) resolveExistingSecretNamespacePolicy(ctx context.Context) (bool, error) {
	var controllers accessv1.ControllerList
	if err := r.List(ctx, &controllers); err != nil {
		return false, err
	}

	switch len(controllers.Items) {
	case 0:
		return false, nil
	case 1:
		return controllers.Items[0].Spec.Settings.ExistingSecretNamespace, nil
	default:
		message := fmt.Sprintf(
			"multiple Controller resources found (%d); exactly one is allowed cluster-wide",
			len(controllers.Items),
		)
		for _, controllerObj := range controllers.Items {
			r.emitEvent(&controllerObj, corev1.EventTypeWarning, multipleControllersFoundReason, message)
		}
		return false, errors.New(message)
	}
}

func (r *PostgresAccessReconciler) emitEvent(object client.Object, eventType, reason, message string) {
	if r.Recorder == nil || object == nil {
		return
	}

	message = fmt.Sprintf("%s (at %s)", message, time.Now().Format(time.RFC3339))

	r.Recorder.Eventf(object, nil, eventType, reason, "PolicyValidation", "%s", message)
}

func getExistingSecretConnectionDetails(ctx context.Context, c client.Client, secretName, namespace string, pg *accessv1.PostgresAccess) (ConnectionDetails, error) {
	var existingSec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, &existingSec); err != nil {
		return ConnectionDetails{}, fmt.Errorf("failed to get existing secret for connection details: %w", err)
	}

	existingUsername, ok := getSecretValue(existingSec.Data, "username", "user")
	if !ok {
		return ConnectionDetails{}, fmt.Errorf("existing secret is missing username (username or user key)")
	}

	existingPassword, ok := getSecretValue(existingSec.Data, "password")
	if !ok {
		return ConnectionDetails{}, fmt.Errorf("existing secret is missing password")
	}

	existingHost, hasHost := getSecretValue(existingSec.Data, "host")
	uriHost, uriPort := getHostAndPortFromURI(existingSec.Data, "fqdn-uri", "uri", "jdbc-uri")
	if !hasHost {
		existingHost = uriHost
	}
	if existingHost == "" {
		return ConnectionDetails{}, fmt.Errorf("existing secret is missing host")
	}
	if uriHost != "" && !isQualifiedHost(existingHost) {
		existingHost = uriHost
	}
	existingHost = qualifyServiceHost(existingHost, namespace)

	existingPort, ok := getSecretValue(existingSec.Data, "port")
	if !ok {
		if uriPort != "" {
			existingPort = uriPort
		} else {
			return ConnectionDetails{}, fmt.Errorf("existing secret is missing port")
		}
	}
	if existingPort == "" {
		return ConnectionDetails{}, fmt.Errorf("existing secret is missing port")
	}

	existingDatabase, ok := getSecretValue(existingSec.Data, "dbname", "database")
	invalidDatabases := []string{"*", "%", "(none)", "null", ""}
	if !ok || slices.Contains(invalidDatabases, strings.ToLower(existingDatabase)) {
		existingDatabase = "postgres"
	}

	if pg != nil {
		conn := pg.Spec.Connection
		if conn.Database != nil && *conn.Database != "" {
			existingDatabase = *conn.Database
		}
	}

	// sslmode is optional, defaults to "require" for security
	sslMode := "require"
	if existingSSLMode, ok := getSecretValue(existingSec.Data, "sslmode"); ok {
		sslMode = existingSSLMode
	}

	return ConnectionDetails{
		Username: existingUsername,
		Password: existingPassword,
		Host:     existingHost,
		Port:     existingPort,
		Database: existingDatabase,
		SSLMode:  sslMode,
	}, nil
}

func getSecretValue(secretData map[string][]byte, keys ...string) (string, bool) {
	for _, key := range keys {
		if data, ok := secretData[key]; ok && len(data) > 0 {
			return strings.TrimSpace(string(data)), true
		}
	}

	return "", false
}

func getHostAndPortFromURI(secretData map[string][]byte, keys ...string) (string, string) {
	for _, key := range keys {
		rawURI, ok := secretData[key]
		if !ok || len(rawURI) == 0 {
			continue
		}

		parsed, err := neturl.Parse(strings.TrimSpace(string(rawURI)))
		if err != nil {
			continue
		}

		host := parsed.Hostname()
		if host == "" {
			continue
		}

		return host, parsed.Port()
	}

	return "", ""
}

func isQualifiedHost(host string) bool {
	return strings.Contains(host, ".") || net.ParseIP(host) != nil || strings.EqualFold(host, "localhost")
}

func qualifyServiceHost(host, namespace string) string {
	if isQualifiedHost(host) || namespace == "" {
		return host
	}

	return fmt.Sprintf("%s.%s.svc", host, namespace)
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
			normalizedGrant := normalizeGrant(grant, privilege)
			key := grantKey(normalizedGrant)
			currentMap[key] = accessv1.GrantSpec{
				Database:   normalizedGrant.Database,
				Schema:     normalizedGrant.Schema,
				Privileges: normalizedGrant.Privileges,
			}
		}
	}

	for _, grant := range desired {
		for _, privilege := range grant.Privileges {
			normalizedGrant := normalizeGrant(grant, privilege)
			key := grantKey(normalizedGrant)
			desiredMap[key] = accessv1.GrantSpec{
				Database:   normalizedGrant.Database,
				Schema:     normalizedGrant.Schema,
				Privileges: normalizedGrant.Privileges,
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

func normalizeGrant(grant accessv1.GrantSpec, privilege string) accessv1.GrantSpec {
	normalized := accessv1.GrantSpec{
		Database:   strings.TrimSpace(grant.Database),
		Privileges: []string{strings.ToUpper(strings.TrimSpace(privilege))},
	}

	schema := defaultSchemaName
	if grant.Schema != nil && strings.TrimSpace(*grant.Schema) != "" {
		schema = strings.TrimSpace(*grant.Schema)
	}
	normalized.Schema = &schema

	return normalized
}

func grantKey(grant accessv1.GrantSpec) string {
	schema := ""
	if grant.Schema != nil {
		schema = strings.TrimSpace(*grant.Schema)
	}

	privilege := ""
	if len(grant.Privileges) > 0 {
		privilege = strings.ToUpper(strings.TrimSpace(grant.Privileges[0]))
	}

	return fmt.Sprintf("%s:%s:%s", strings.TrimSpace(grant.Database), schema, privilege)
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
