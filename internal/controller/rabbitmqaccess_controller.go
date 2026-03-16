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
	"maps"

	"github.com/go-logr/logr"
	rabbithole "github.com/michaelklishin/rabbit-hole/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

type RabbitMQUserConfig struct {
	Password    string
	Permissions []accessv1.RabbitMQPermissionSpec
}

const (
	rabbitMQAccessReadyConditionType      = "Ready"
	rabbitMQAccessSuccessConditionType    = "Success"
	rabbitMQAccessInProgressConditionType = "InProgress"
)

func rabbitMQReconcileStatusConfig() reconcileStatusConfig[*accessv1.RabbitMQAccess] {
	return reconcileStatusConfig[*accessv1.RabbitMQAccess]{
		newObject: func() *accessv1.RabbitMQAccess {
			return &accessv1.RabbitMQAccess{}
		},
		conditions: func(obj *accessv1.RabbitMQAccess) *[]metav1.Condition {
			return &obj.Status.Conditions
		},
		setLastLog: func(obj *accessv1.RabbitMQAccess, message string) {
			obj.Status.LastLog = message
		},
		setLastReconcileState: func(obj *accessv1.RabbitMQAccess, state accessv1.ReconcileState) {
			obj.Status.LastReconcileState = state
		},
		conditionTypes: reconcileConditionTypes{
			Ready:      rabbitMQAccessReadyConditionType,
			Success:    rabbitMQAccessSuccessConditionType,
			InProgress: rabbitMQAccessInProgressConditionType,
		},
	}
}

// RabbitMQAccessReconciler reconciles a RabbitMQAccess object
type RabbitMQAccessReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=rabbitmqaccesses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=rabbitmqaccesses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=rabbitmqaccesses/finalizers,verbs=update
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=controllers,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
func (r *RabbitMQAccessReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var rbq accessv1.RabbitMQAccess
	if err := r.Get(ctx, req.NamespacedName, &rbq); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	inSync := true

	_, passwordReused, err := reconcileGeneratedCredentialsSecret(
		ctx,
		r.Client,
		r.Scheme,
		&rbq,
		rbq.Spec.GeneratedSecret,
		req.Namespace,
		rbq.Spec.Username,
	)
	if err != nil {
		log.Error(err, "failed to create/update secret", "secret", rbq.Spec.GeneratedSecret)
		r.emitWarningEvent(&rbq, "SecretSyncFailed", err.Error())
		statusErr := setReconcileStatus(
			ctx,
			r.Client,
			req.NamespacedName,
			rabbitMQReconcileStatusConfig(),
			accessv1.ReconcileStateError,
			"SecretSyncFailed",
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

	rbmqSync, reason, err := reconcileRabbitMQ(ctx, r, &rbq, log)
	if err != nil {
		r.emitWarningEvent(&rbq, "CreateError", "Failed to create missing users or vhosts: "+err.Error())
		_ = setReconcileStatus(
			ctx,
			r.Client,
			req.NamespacedName,
			rabbitMQReconcileStatusConfig(),
			accessv1.ReconcileStateError,
			reason,
			err.Error(),
		)

		log.Error(err, "failed to reconcile RabbitMQAccess", "name", rbq.Name)
		return ctrl.Result{}, err
	}
	if !rbmqSync {
		inSync = false
	}

	if !inSync {
		if err := setReconcileStatus(
			ctx,
			r.Client,
			req.NamespacedName,
			rabbitMQReconcileStatusConfig(),
			accessv1.ReconcileStateInProgress,
			"Reconciling",
			"RabbitMQAccess is not yet in sync",
		); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: privilegeDriftRequeueInterval}, nil
	}

	if err := setReconcileStatus(
		ctx,
		r.Client,
		req.NamespacedName,
		rabbitMQReconcileStatusConfig(),
		accessv1.ReconcileStateSuccess,
		"Ready",
		"RabbitMQAccess is in sync",
	); err != nil {
		return ctrl.Result{RequeueAfter: syncedRequeueInterval}, nil
	}

	return ctrl.Result{RequeueAfter: privilegeDriftRequeueInterval}, nil
}

func reconcileRabbitMQ(ctx context.Context, r *RabbitMQAccessReconciler, rbq *accessv1.RabbitMQAccess, log logr.Logger) (bool, string, error) {
	insync := true

	rmqc, err := r.initializeRabbitMQClientConnection(ctx, rbq)
	if err != nil {
		return false, "connectionError", fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	usersPermissions, err := r.ListUsersAndPermissions(rmqc)
	if err != nil {
		return false, "listError", fmt.Errorf("failed to list users and permissions: %w", err)
	}

	desiredUsers, err := r.getAllRabbitMQUserConfigs(ctx)
	if err != nil {
		return false, "ListCRsError", fmt.Errorf("failed to list RabbitMQAccess CRs: %w", err)
	}
	connectionUsers, err := r.getAllRabbitMQConnectionUsernames(ctx)
	if err != nil {
		return false, "ListCRsError", fmt.Errorf("failed to resolve RabbitMQ connection usernames: %w", err)
	}

	excludedUsers, err := r.resolveExcludedUsers(ctx)
	if err != nil {
		return false, multipleControllersFoundReason, fmt.Errorf("failed to resolve excluded users: %w", err)
	}

	usersAndVhostsInSync, reason, err := r.reconcileUsersAndVhosts(rmqc, desiredUsers, usersPermissions, excludedUsers, log)
	if err != nil {
		return false, reason, err
	}
	if !usersAndVhostsInSync {
		insync = false
	}

	unhandledUsers := make(map[string][]accessv1.RabbitMQPermissionSpec, len(usersPermissions))
	maps.Copy(unhandledUsers, usersPermissions)
	for username := range excludedUsers {
		delete(unhandledUsers, username)
	}
	for username := range connectionUsers {
		delete(unhandledUsers, username)
	}
	for username := range desiredUsers {
		delete(unhandledUsers, username)
	}

	// delete users that are not referenced by any CR
	for username := range unhandledUsers {
		if err := r.DeleteUser(rmqc, username); err != nil {
			return false, "DeleteError", fmt.Errorf("failed to delete user %s: %w", username, err)
		}
		insync = false
	}

	return insync, "", nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RabbitMQAccessReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&accessv1.RabbitMQAccess{}).
		Owns(&corev1.Secret{}).
		Named("rabbitmqaccess").
		Complete(r)
}

func (r *RabbitMQAccessReconciler) emitWarningEvent(object client.Object, reason, message string) {
	emitEvent(r.Recorder, object, corev1.EventTypeWarning, reason, message)
}

func (r *RabbitMQAccessReconciler) getAllRabbitMQUserConfigs(ctx context.Context) (map[string]RabbitMQUserConfig, error) {
	var rbqs accessv1.RabbitMQAccessList
	if err := r.List(ctx, &rbqs); err != nil {
		return nil, err
	}

	configs := make(map[string]RabbitMQUserConfig, len(rbqs.Items))
	for i := range rbqs.Items {
		rbq := &rbqs.Items[i]

		password, _, err := reconcileGeneratedCredentialsSecret(
			ctx,
			r.Client,
			r.Scheme,
			rbq,
			rbq.Spec.GeneratedSecret,
			rbq.Namespace,
			rbq.Spec.Username,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to reconcile generated secret for RabbitMQAccess %s/%s: %w",
				rbq.Namespace,
				rbq.Name,
				err,
			)
		}

		config := configs[rbq.Spec.Username]
		if config.Password == "" {
			config.Password = password
		}
		config.Permissions = append(config.Permissions, rbq.Spec.Permissions...)
		configs[rbq.Spec.Username] = config
	}

	return configs, nil
}

func (r *RabbitMQAccessReconciler) getAllRabbitMQConnectionUsernames(ctx context.Context) (map[string]struct{}, error) {
	var rbqs accessv1.RabbitMQAccessList
	if err := r.List(ctx, &rbqs); err != nil {
		return nil, err
	}

	usernames := make(map[string]struct{}, len(rbqs.Items))
	for i := range rbqs.Items {
		rbq := &rbqs.Items[i]
		connection, err := r.getConnectionDetails(ctx, rbq)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to resolve connection details for RabbitMQAccess %s/%s: %w",
				rbq.Namespace,
				rbq.Name,
				err,
			)
		}
		if connection.Username == "" {
			continue
		}
		usernames[connection.Username] = struct{}{}
	}

	return usernames, nil
}

func (r *RabbitMQAccessReconciler) reconcileUsersAndVhosts(
	rmqc *rabbithole.Client,
	desiredUsers map[string]RabbitMQUserConfig,
	currentPermissions map[string][]accessv1.RabbitMQPermissionSpec,
	excludedUsers map[string]struct{},
	log logr.Logger,
) (bool, string, error) {
	const createErrorReason = "CreateError"

	inSync := true

	for username, desiredUser := range desiredUsers {
		if _, excluded := excludedUsers[username]; excluded {
			log.Info("Skipping excluded RabbitMQ user", "username", username)
			continue
		}

		if _, exists := currentPermissions[username]; !exists {
			inSync = false
		}

		if err := r.CreateUser(rmqc, username, desiredUser.Password); err != nil {
			if _, exists := currentPermissions[username]; exists {
				return false, createErrorReason, fmt.Errorf("failed to update user %s: %w", username, err)
			}
			return false, createErrorReason, fmt.Errorf("failed to create user %s: %w", username, err)
		}

		for _, perm := range desiredUser.Permissions {
			if exists, err := r.vhostExists(rmqc, perm.VHost); err != nil {
				return false, createErrorReason, fmt.Errorf("failed to check if vhost %s exists: %w", perm.VHost, err)
			} else if !exists {
				inSync = false
				if err = r.CreateVhost(rmqc, perm.VHost); err != nil {
					return false, createErrorReason, fmt.Errorf("failed to create vhost %s: %w", perm.VHost, err)
				}
			}
		}

		if !permissionsEqual(desiredUser.Permissions, currentPermissions[username]) {
			inSync = false
		}
		if err := r.SetPermissionsExact(rmqc, username, desiredUser.Permissions, currentPermissions[username]); err != nil {
			return false, "GrantError", fmt.Errorf("failed to grant permissions for user %s: %w", username, err)
		}
	}

	return inSync, "", nil
}

func resolveRabbitMQControllerSettings(ctx context.Context, r *RabbitMQAccessReconciler) (accessv1.ControllerSettings, error) {
	return resolveControllerSettings(ctx, r.Client, func(controllerObj *accessv1.Controller, message string) {
		r.emitWarningEvent(controllerObj, multipleControllersFoundReason, message)
	})
}

func permissionsEqual(desired, current []accessv1.RabbitMQPermissionSpec) bool {
	if len(desired) != len(current) {
		return false
	}

	desiredMapStringString := make(map[string]string, len(desired))
	currentMapStringString := make(map[string]string, len(current))

	for i := range desired {
		desiredMapStringString[desired[i].VHost] = fmt.Sprintf("conf:%s,write:%s,read:%s", desired[i].Configure, desired[i].Write, desired[i].Read)
	}

	for i := range current {
		currentMapStringString[current[i].VHost] = fmt.Sprintf("conf:%s,write:%s,read:%s", current[i].Configure, current[i].Write, current[i].Read)
	}

	return maps.Equal(desiredMapStringString, currentMapStringString)
}
