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

package redis

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/delta10/access-operator/internal/controller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

const redisAccessFinalizer = controller.AccessResourceFinalizer

const (
	redisAccessConnectionErrorReason = "ConnectionError"
	redisAccessListErrorReason       = "ListError"
	redisAccessListCRsErrorReason    = "ListCRsError"
	redisAccessACLValidationError    = "ACLValidationError"
	redisAccessApplyErrorReason      = "ApplyError"
	redisAccessDeleteErrorReason     = "DeleteError"
	redisAccessFinalizeErrorReason   = "FinalizeError"
	redisACLCompareUserNamePrefix    = "user"
	redisTemporaryUserNamePrefix     = "access-operator-compare"
)

// UserConfig is the aggregated desired Redis ACL config for a username.
type UserConfig struct {
	Password string
	ACLRules []string
}

// RedisAccessReconciler reconciles a RedisAccess object.
type RedisAccessReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Recorder     events.EventRecorder
	NewACLClient ClientFactory
}

func redisReconcileStatusConfig() controller.ReconcileStatusConfig[*accessv1.RedisAccess] {
	return controller.NewStandardReconcileStatusConfig(
		func() *accessv1.RedisAccess {
			return &accessv1.RedisAccess{}
		},
		func(obj *accessv1.RedisAccess) *[]metav1.Condition {
			return &obj.Status.Conditions
		},
		func(obj *accessv1.RedisAccess, message string) {
			obj.Status.LastLog = message
		},
		func(obj *accessv1.RedisAccess, state accessv1.ReconcileState) {
			obj.Status.LastReconcileState = state
		},
	)
}

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=redisaccesses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=redisaccesses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=redisaccesses/finalizers,verbs=update
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=controllers,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop.
func (r *RedisAccessReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return controller.ReconcileManagedAccess(ctx, req, controller.ManagedAccessReconcileConfig[*accessv1.RedisAccess]{
		Client:       r.Client,
		Scheme:       r.Scheme,
		StatusConfig: redisReconcileStatusConfig(),
		Finalize:     r.finalizeRedisAccess,
		Sync: func(ctx context.Context, redisAccess *accessv1.RedisAccess) (bool, string, error) {
			return r.reconcileRedisAccess(ctx, redisAccess)
		},
		SecretName: func(redisAccess *accessv1.RedisAccess) string {
			return redisAccess.Spec.GeneratedSecret
		},
		Username: func(redisAccess *accessv1.RedisAccess) string {
			return redisAccess.Spec.Username
		},
		EmitEvent: func(redisAccess *accessv1.RedisAccess, eventType, reason, message string) {
			controller.EmitEvent(r.Recorder, redisAccess, eventType, reason, message)
		},
		FinalizeErrorReason: redisAccessFinalizeErrorReason,
		SyncErrorEventText: func(_ *accessv1.RedisAccess, _ string, err error) string {
			return "Failed to reconcile RedisAccess: " + err.Error()
		},
		InProgressMessage: "RedisAccess is not yet in sync",
		SuccessMessage:    "RedisAccess is in sync",
	})
}

func (r *RedisAccessReconciler) finalizeRedisAccess(ctx context.Context, redisAccess *accessv1.RedisAccess) (bool, error) {
	log := logf.FromContext(ctx)

	if redisAccess.DeletionTimestamp.IsZero() {
		if err := controller.AddAccessFinalizerIfMissing(ctx, r.Client, redisAccess); err != nil {
			return false, err
		}
		return false, nil
	}

	if !controllerutil.ContainsFinalizer(redisAccess, redisAccessFinalizer) {
		return true, nil
	}

	excludedUsers, err := r.resolveExcludedUsers(ctx)
	if err != nil {
		return true, err
	}
	if _, excluded := excludedUsers[redisAccess.Spec.Username]; excluded {
		log.Info("Skipping finalizer Redis cleanup for excluded user", "username", redisAccess.Spec.Username)
		return true, controller.RemoveAccessFinalizerIfPresent(ctx, r.Client, redisAccess)
	}

	connectionUsers, err := r.getAllRedisConnectionUsernames(ctx)
	if err != nil {
		return true, fmt.Errorf("failed to resolve Redis connection usernames during finalization: %w", err)
	}
	if _, inUseByConnection := connectionUsers[redisAccess.Spec.Username]; inUseByConnection {
		log.Info("Skipping finalizer Redis user deletion because it is used for Redis connections", "username", redisAccess.Spec.Username)
		return true, controller.RemoveAccessFinalizerIfPresent(ctx, r.Client, redisAccess)
	}

	remainingUsers, err := r.getRemainingRedisUserConfigs(ctx, client.ObjectKeyFromObject(redisAccess))
	if err != nil {
		return true, fmt.Errorf("failed to list remaining RedisAccess resources during finalization: %w", err)
	}
	if _, stillDesired := remainingUsers[redisAccess.Spec.Username]; stillDesired {
		log.Info("Skipping finalizer Redis user deletion because another RedisAccess still manages it", "username", redisAccess.Spec.Username)
		return true, controller.RemoveAccessFinalizerIfPresent(ctx, r.Client, redisAccess)
	}

	connection, err := r.getConnectionDetails(ctx, redisAccess)
	if err != nil {
		return true, fmt.Errorf("failed to resolve Redis connection during finalization: %w", err)
	}

	aclClient := r.newACLClient(connection.SharedConnectionDetails)
	defer func() {
		_ = aclClient.Close()
	}()

	if err := aclClient.Ping(ctx); err != nil {
		return true, fmt.Errorf("failed to connect to Redis during finalization: %w", err)
	}

	users, err := aclClient.ListUsers(ctx)
	if err != nil {
		return true, fmt.Errorf("failed to list Redis users during finalization: %w", err)
	}

	if slices.Contains(users, redisAccess.Spec.Username) {
		if _, err := aclClient.DeleteUser(ctx, redisAccess.Spec.Username); err != nil {
			return true, fmt.Errorf("failed to delete Redis user %s during finalization: %w", redisAccess.Spec.Username, err)
		}
	}

	return true, controller.RemoveAccessFinalizerIfPresent(ctx, r.Client, redisAccess)
}

func (r *RedisAccessReconciler) reconcileRedisAccess(
	ctx context.Context,
	redisAccess *accessv1.RedisAccess,
) (bool, string, error) {
	log := logf.FromContext(ctx)

	connection, err := r.getConnectionDetails(ctx, redisAccess)
	if err != nil {
		return false, redisAccessConnectionErrorReason, fmt.Errorf("failed to resolve Redis connection details: %w", err)
	}

	aclClient := r.newACLClient(connection.SharedConnectionDetails)
	defer func() {
		_ = aclClient.Close()
	}()

	if err := aclClient.Ping(ctx); err != nil {
		return false, redisAccessConnectionErrorReason, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	desiredUsers, err := r.getAllRedisUserConfigs(ctx)
	if err != nil {
		return false, redisAccessListCRsErrorReason, fmt.Errorf("failed to list RedisAccess resources: %w", err)
	}

	connectionUsers, err := r.getAllRedisConnectionUsernames(ctx)
	if err != nil {
		return false, redisAccessListCRsErrorReason, fmt.Errorf("failed to resolve Redis connection usernames: %w", err)
	}

	excludedUsers, err := r.resolveExcludedUsers(ctx)
	if err != nil {
		return false, controller.MultipleControllersFoundReason, fmt.Errorf("failed to resolve excluded users: %w", err)
	}

	currentUsers, err := aclClient.ListUsers(ctx)
	if err != nil {
		return false, redisAccessListErrorReason, fmt.Errorf("failed to list Redis users: %w", err)
	}

	currentACL, err := aclClient.ListACL(ctx)
	if err != nil {
		return false, redisAccessListErrorReason, fmt.Errorf("failed to list Redis ACLs: %w", err)
	}

	currentACLByUser := aclLinesByUser(currentACL)
	inSync := true

	desiredUsernames := sortedMapKeys(desiredUsers)
	for _, username := range desiredUsernames {
		if _, excluded := excludedUsers[username]; excluded {
			log.Info("Skipping excluded Redis user", "username", username)
			continue
		}

		desiredUser := desiredUsers[username]
		if err := validateACLRules(desiredUser.ACLRules); err != nil {
			return false, redisAccessACLValidationError, fmt.Errorf("invalid aclRules for user %s: %w", username, err)
		}

		desiredACL, err := r.canonicalizeDesiredACL(ctx, aclClient, desiredUser.Password, desiredUser.ACLRules)
		if err != nil {
			return false, redisAccessACLValidationError, fmt.Errorf("failed to canonicalize ACL rules for user %s: %w", username, err)
		}

		currentACLState, exists := currentACLByUser[username]
		if !exists || currentACLState != desiredACL {
			inSync = false
			if err := aclClient.SetUser(ctx, username, buildManagedACLRules(desiredUser.Password, desiredUser.ACLRules)...); err != nil {
				return false, redisAccessApplyErrorReason, fmt.Errorf("failed to apply Redis ACL for user %s: %w", username, err)
			}
		}
	}

	for _, username := range currentUsers {
		if _, desired := desiredUsers[username]; desired {
			continue
		}
		if _, excluded := excludedUsers[username]; excluded {
			log.Info("Skipping excluded Redis user cleanup", "username", username)
			continue
		}
		if _, protected := connectionUsers[username]; protected {
			log.Info("Skipping Redis user cleanup because it is used for Redis connections", "username", username)
			continue
		}

		deleted, err := aclClient.DeleteUser(ctx, username)
		if err != nil {
			return false, redisAccessDeleteErrorReason, fmt.Errorf("failed to delete Redis user %s: %w", username, err)
		}
		if deleted > 0 {
			inSync = false
		}
	}

	return inSync, "", nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisAccessReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&accessv1.RedisAccess{}).
		Owns(&corev1.Secret{}).
		Named("redisaccess").
		Complete(r)
}

func (r *RedisAccessReconciler) newACLClient(connection controller.SharedConnectionDetails) ACLClient {
	if r.NewACLClient != nil {
		return r.NewACLClient(connection)
	}

	return NewClientFactory()(connection)
}

func (r *RedisAccessReconciler) getAllRedisUserConfigs(ctx context.Context) (map[string]UserConfig, error) {
	var redisAccesses accessv1.RedisAccessList
	if err := r.List(ctx, &redisAccesses); err != nil {
		return nil, err
	}

	sort.Slice(redisAccesses.Items, func(i, j int) bool {
		if redisAccesses.Items[i].Namespace == redisAccesses.Items[j].Namespace {
			return redisAccesses.Items[i].Name < redisAccesses.Items[j].Name
		}
		return redisAccesses.Items[i].Namespace < redisAccesses.Items[j].Namespace
	})

	configs := make(map[string]UserConfig, len(redisAccesses.Items))
	for i := range redisAccesses.Items {
		redisAccess := &redisAccesses.Items[i]
		if !redisAccess.DeletionTimestamp.IsZero() {
			continue
		}

		password, _, err := controller.ReconcileGeneratedCredentialsSecret(
			ctx,
			r.Client,
			r.Scheme,
			redisAccess,
			redisAccess.Spec.GeneratedSecret,
			redisAccess.Namespace,
			redisAccess.Spec.Username,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to reconcile generated secret for RedisAccess %s/%s: %w",
				redisAccess.Namespace,
				redisAccess.Name,
				err,
			)
		}

		config := configs[redisAccess.Spec.Username]
		if config.Password == "" {
			config.Password = password
		}
		config.ACLRules = append(config.ACLRules, redisAccess.Spec.ACLRules...)
		configs[redisAccess.Spec.Username] = config
	}

	return configs, nil
}

func (r *RedisAccessReconciler) getRemainingRedisUserConfigs(
	ctx context.Context,
	excludedKey client.ObjectKey,
) (map[string]UserConfig, error) {
	var redisAccesses accessv1.RedisAccessList
	if err := r.List(ctx, &redisAccesses); err != nil {
		return nil, err
	}

	sort.Slice(redisAccesses.Items, func(i, j int) bool {
		if redisAccesses.Items[i].Namespace == redisAccesses.Items[j].Namespace {
			return redisAccesses.Items[i].Name < redisAccesses.Items[j].Name
		}
		return redisAccesses.Items[i].Namespace < redisAccesses.Items[j].Namespace
	})

	configs := make(map[string]UserConfig, len(redisAccesses.Items))
	for i := range redisAccesses.Items {
		redisAccess := &redisAccesses.Items[i]
		if client.ObjectKeyFromObject(redisAccess) == excludedKey || !redisAccess.DeletionTimestamp.IsZero() {
			continue
		}

		config := configs[redisAccess.Spec.Username]
		config.ACLRules = append(config.ACLRules, redisAccess.Spec.ACLRules...)
		configs[redisAccess.Spec.Username] = config
	}

	return configs, nil
}

func (r *RedisAccessReconciler) getAllRedisConnectionUsernames(ctx context.Context) (map[string]struct{}, error) {
	var redisAccesses accessv1.RedisAccessList
	if err := r.List(ctx, &redisAccesses); err != nil {
		return nil, err
	}

	usernames := make(map[string]struct{}, len(redisAccesses.Items))
	for i := range redisAccesses.Items {
		redisAccess := &redisAccesses.Items[i]
		if !redisAccess.DeletionTimestamp.IsZero() {
			continue
		}

		connection, err := r.getConnectionDetails(ctx, redisAccess)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to resolve connection details for RedisAccess %s/%s: %w",
				redisAccess.Namespace,
				redisAccess.Name,
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

func (r *RedisAccessReconciler) canonicalizeDesiredACL(
	ctx context.Context,
	aclClient ACLClient,
	password string,
	aclRules []string,
) (string, error) {
	tempUserName := fmt.Sprintf("%s-%d", redisTemporaryUserNamePrefix, time.Now().UnixNano())
	if err := aclClient.SetUser(ctx, tempUserName, buildManagedACLRules(password, aclRules)...); err != nil {
		return "", err
	}
	defer func() {
		_, _ = aclClient.DeleteUser(ctx, tempUserName)
	}()

	aclLines, err := aclClient.ListACL(ctx)
	if err != nil {
		return "", err
	}

	line, ok := aclLinesByUser(aclLines)[tempUserName]
	if !ok {
		return "", fmt.Errorf("temporary Redis ACL user %s was not returned by ACL LIST", tempUserName)
	}

	return line, nil
}

func buildManagedACLRules(password string, aclRules []string) []string {
	rules := make([]string, 0, 3+len(aclRules))
	rules = append(rules, "reset", "on", ">"+password)
	rules = append(rules, aclRules...)
	return rules
}

func validateACLRules(aclRules []string) error {
	if len(aclRules) == 0 {
		return fmt.Errorf("aclRules must contain at least one rule")
	}

	for _, rawRule := range aclRules {
		rule := strings.TrimSpace(rawRule)
		if rule == "" {
			return fmt.Errorf("aclRules cannot contain empty entries")
		}

		lowerRule := strings.ToLower(rule)
		switch lowerRule {
		case "reset", "on", "off", "nopass", "resetpass":
			return fmt.Errorf("acl rule %q is reserved for controller-managed authentication", rule)
		}

		switch rule[0] {
		case '>', '<', '#', '!':
			return fmt.Errorf("acl rule %q is reserved for controller-managed authentication", rule)
		}
	}

	return nil
}

func aclLinesByUser(lines []string) map[string]string {
	byUser := make(map[string]string, len(lines))
	for _, line := range lines {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 3 || fields[0] != redisACLCompareUserNamePrefix {
			continue
		}
		byUser[fields[1]] = strings.Join(fields[2:], " ")
	}
	return byUser
}

func sortedMapKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
