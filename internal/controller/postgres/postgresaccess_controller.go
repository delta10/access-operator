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

package postgres

import (
	"context"
	"fmt"
	"slices"

	"github.com/delta10/access-operator/internal/controller"
	corev1 "k8s.io/api/core/v1"
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

const postgresAccessFinalizer = controller.AccessResourceFinalizer

func postgresReconcileStatusConfig() controller.ReconcileStatusConfig[*accessv1.PostgresAccess] {
	return controller.NewStandardReconcileStatusConfig(
		func() *accessv1.PostgresAccess {
			return &accessv1.PostgresAccess{}
		},
		func(obj *accessv1.PostgresAccess) *[]metav1.Condition {
			return &obj.Status.Conditions
		},
		func(obj *accessv1.PostgresAccess, message string) {
			obj.Status.LastLog = message
		},
		func(obj *accessv1.PostgresAccess, state accessv1.ReconcileState) {
			obj.Status.LastReconcileState = state
		},
	)
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
	return controller.ReconcileManagedAccess(ctx, req, controller.ManagedAccessReconcileConfig[*accessv1.PostgresAccess]{
		Client:       r.Client,
		Scheme:       r.Scheme,
		StatusConfig: postgresReconcileStatusConfig(),
		Finalize:     r.finalizePostgresAccess,
		Sync: func(ctx context.Context, pg *accessv1.PostgresAccess) (bool, string, error) {
			inSync, err := r.reconcilePostgresAccess(ctx, pg)
			return inSync, "DatabaseSyncFailed", err
		},
		SecretName: func(pg *accessv1.PostgresAccess) string {
			return pg.Spec.GeneratedSecret
		},
		Username: func(pg *accessv1.PostgresAccess) string {
			return pg.Spec.Username
		},
		EmitEvent: func(pg *accessv1.PostgresAccess, eventType, reason, message string) {
			r.emitEvent(pg, eventType, reason, message)
		},
		FinalizeErrorReason: "FinalizeFailed",
		SyncErrorReason:     "DatabaseSyncFailed",
		InProgressMessage:   "PostgresAccess is not yet in sync",
		SuccessMessage:      "PostgresAccess is in sync",
		SuccessEventReason:  "ReconcileSuccess",
		SuccessEventMessage: "PostgresAccess is in sync and ready",
	})
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
	staleUserDeletionPolicy, err := r.resolveStaleUserDeletionPolicy(ctx)
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
	if staleUserDeletionPolicy != accessv1.CleanupPolicyRestrict {
		for _, user := range users {
			if !usersHandled[user] {
				err = r.DB.DropUser(ctx, user, staleUserDeletionPolicy)
				if err != nil {
					log.Error(err, "failed to drop user in PostgreSQL", "username", user)
					inSync = false
					continue
				}
			}
		}
	}

	return inSync, nil
}

func (r *PostgresAccessReconciler) finalizePostgresAccess(ctx context.Context, pg *accessv1.PostgresAccess) (bool, error) {
	log := logf.FromContext(ctx)
	if pg.DeletionTimestamp.IsZero() {
		if err := controller.AddAccessFinalizerIfMissing(ctx, r.Client, pg); err != nil {
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
		if err := controller.RemoveAccessFinalizerIfPresent(ctx, r.Client, pg); err != nil {
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

	staleUserDeletionPolicy, err := r.resolveStaleUserDeletionPolicy(ctx)
	if err != nil {
		return true, err
	}

	if staleUserDeletionPolicy != accessv1.CleanupPolicyRestrict {
		for _, user := range users {
			if pg.Spec.Username == user {
				err = r.DB.DropUser(ctx, user, staleUserDeletionPolicy)
				if err != nil {
					log.Error(err, "failed to drop user in PostgreSQL during finalization", "username", user)
					continue
				}
			}
		}
	} else {
		log.Info("Skipping finalizer PostgreSQL user deletion because stale user deletion policy is Restrict", "username", pg.Spec.Username)
	}

	if err := controller.RemoveAccessFinalizerIfPresent(ctx, r.Client, pg); err != nil {
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
	controller.EmitEvent(r.Recorder, object, eventType, reason, message)
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
	return controller.ResolveControllerSettings(ctx, r.Client, func(controllerObj *accessv1.Controller, message string) {
		r.emitEvent(controllerObj, "Warning", controller.MultipleControllersFoundReason, message)
	})
}
