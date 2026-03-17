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
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

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

const postgresAccessFinalizer = accessResourceFinalizer

func postgresReconcileStatusConfig() reconcileStatusConfig[*accessv1.PostgresAccess] {
	return reconcileStatusConfig[*accessv1.PostgresAccess]{
		newObject: func() *accessv1.PostgresAccess {
			return &accessv1.PostgresAccess{}
		},
		conditions: func(obj *accessv1.PostgresAccess) *[]metav1.Condition {
			return &obj.Status.Conditions
		},
		setLastLog: func(obj *accessv1.PostgresAccess, message string) {
			obj.Status.LastLog = message
		},
		setLastReconcileState: func(obj *accessv1.PostgresAccess, state accessv1.ReconcileState) {
			obj.Status.LastReconcileState = state
		},
		conditionTypes: reconcileConditionTypes{
			Ready:      ReadyConditionType,
			Success:    SuccessConditionType,
			InProgress: InProgressConditionType,
		},
	}
}

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

	inSync := true

	finalized, err := r.finalizePostgresAccess(ctx, &pg)
	if err != nil {
		r.emitEvent(&pg, corev1.EventTypeWarning, "FinalizeFailed", fmt.Sprintf("Finalization failed: %v", err))
		statusErr := setReconcileStatus(
			ctx,
			r.Client,
			req.NamespacedName,
			postgresReconcileStatusConfig(),
			accessv1.ReconcileStateError,
			"FinalizeFailed",
			fmt.Sprintf("Finalization failed: %v", err),
		)
		if statusErr != nil {
			log.Error(statusErr, "failed to update status after finalization failure")
		}
		return ctrl.Result{}, err
	}
	// If the resource is finalized, we return early to avoid requeuing.
	// The finalizer will have been removed, so no further reconciliation will occur for this resource.
	if finalized {
		return ctrl.Result{}, nil
	}

	// reconcile the secret that holds the connection details for this PostgresAccess CR.
	_, passwordReused, err := reconcileGeneratedCredentialsSecret(
		ctx,
		r.Client,
		r.Scheme,
		&pg,
		pg.Spec.GeneratedSecret,
		req.Namespace,
		pg.Spec.Username,
	)
	if err != nil {
		log.Error(err, "failed to create/update secret", "secret", pg.Spec.GeneratedSecret)
		r.emitEvent(&pg, corev1.EventTypeWarning, SecretSyncErrorEventReason, err.Error())
		statusErr := setReconcileStatus(
			ctx,
			r.Client,
			req.NamespacedName,
			postgresReconcileStatusConfig(),
			accessv1.ReconcileStateError,
			SecretSyncErrorEventReason,
			err.Error(),
		)
		if statusErr != nil {
			log.Error(statusErr, "failed to update status after secret sync failure")
		}
		return ctrl.Result{}, err
	}
	if !passwordReused {
		inSync = false
	}

	pgSync, err := r.reconcilePostgresAccess(ctx, &pg)
	if err != nil {
		log.Error(err, "failed to reconcile PostgresAccess")
		r.emitEvent(&pg, corev1.EventTypeWarning, "DatabaseSyncFailed", err.Error())
		statusErr := setReconcileStatus(
			ctx,
			r.Client,
			req.NamespacedName,
			postgresReconcileStatusConfig(),
			accessv1.ReconcileStateError,
			"DatabaseSyncFailed",
			err.Error(),
		)
		if statusErr != nil {
			log.Error(statusErr, "failed to update status after database sync failure")
		}
		return ctrl.Result{}, err
	}

	if inSync {
		inSync = pgSync
	}

	if inSync {
		if err := setReconcileStatus(
			ctx,
			r.Client,
			req.NamespacedName,
			postgresReconcileStatusConfig(),
			accessv1.ReconcileStateSuccess,
			"Ready",
			"PostgresAccess is in sync",
		); err != nil {
			return ctrl.Result{}, err
		}

		r.emitEvent(&pg, corev1.EventTypeNormal, "ReconcileSuccess", "PostgresAccess is in sync and ready")

		return ctrl.Result{RequeueAfter: syncedRequeueInterval}, nil
	}

	if err := setReconcileStatus(
		ctx,
		r.Client,
		req.NamespacedName,
		postgresReconcileStatusConfig(),
		accessv1.ReconcileStateInProgress,
		"Reconciling",
		"PostgresAccess is not yet in sync",
	); err != nil {
		return ctrl.Result{}, err
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

	excludedUsers, err := r.resolveExcludedUsers(ctx)
	if err != nil {
		return false, err
	}

	usersHandled := make(map[string]bool)
	for username := range excludedUsers {
		usersHandled[username] = true
	}
	inSync := true

	for _, config := range configs {
		if _, excluded := excludedUsers[config.Username]; excluded {
			log.Info("Skipping excluded PostgreSQL user", "username", config.Username)
			continue
		}

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
		if err := addAccessFinalizerIfMissing(ctx, r.Client, pg); err != nil {
			return false, err
		}
		return false, nil
	}

	if !controllerutil.ContainsFinalizer(pg, postgresAccessFinalizer) {
		return true, nil
	}

	excludedUsers, err := r.resolveExcludedUsers(ctx)
	if err != nil {
		return true, err
	}
	if _, excluded := excludedUsers[pg.Spec.Username]; excluded {
		log.Info("Skipping finalizer database cleanup for excluded PostgreSQL user", "username", pg.Spec.Username)
		if err := removeAccessFinalizerIfPresent(ctx, r.Client, pg); err != nil {
			return true, err
		}
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

	if err := removeAccessFinalizerIfPresent(ctx, r.Client, pg); err != nil {
		return true, err
	}

	return true, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresAccessReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&accessv1.PostgresAccess{}).
		Named("postgresaccess").
		Owns(&corev1.Secret{}).
		Complete(r)
}

func (r *PostgresAccessReconciler) emitEvent(object client.Object, eventType, reason, message string) {
	emitEvent(r.Recorder, object, eventType, reason, message)
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

func resolvePostgresControllerSettings(ctx context.Context, r *PostgresAccessReconciler) (accessv1.ControllerSettings, error) {
	return resolveControllerSettings(ctx, r.Client, func(controllerObj *accessv1.Controller, message string) {
		r.emitEvent(controllerObj, "Warning", multipleControllersFoundReason, message)
	})
}
