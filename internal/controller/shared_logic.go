package controller

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	neturl "net/url"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	accessv1 "github.com/delta10/access-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Define constants for requeue intervals because when postgres changes something a reconcile loop won't be triggered so we need to check periodically.
const privilegeDriftRequeueInterval = 30 * time.Second
const syncedRequeueInterval = 5 * time.Minute
const accessResourceFinalizer = "access.k8s.delta10.nl/finalizer"

type controllerMultipleHandler func(*accessv1.Controller, string)

type SharedConnectionDetails struct {
	Username string
	Password string
	Host     string
	Port     string
}

type reconcileConditionTypes struct {
	Ready      string
	Success    string
	InProgress string
}

const (
	ReadyConditionType      = "Ready"
	SuccessConditionType    = "ReconcileSuccess"
	InProgressConditionType = "ReconcileInProgress"

	// Shared reasons
	SecretSyncErrorEventReason = "SecretSyncFailed"
)

type reconcileStatusConfig[T client.Object] struct {
	newObject             func() T
	conditions            func(T) *[]metav1.Condition
	setLastLog            func(T, string)
	setLastReconcileState func(T, accessv1.ReconcileState)
	conditionTypes        reconcileConditionTypes
}

func reconcileGeneratedCredentialsSecret(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	secretName,
	secretNamespace,
	username string,
) (string, bool, error) {
	if secretName == "" {
		return "", false, fmt.Errorf("generated secret name is empty")
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: secretNamespace,
		},
	}

	passwordReused := true
	var password string

	_, err := controllerutil.CreateOrUpdate(ctx, c, secret, func() error {
		secret.Type = corev1.SecretTypeOpaque

		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}

		existingPassword, ok := secret.Data["password"]
		if ok && len(existingPassword) > 0 {
			password = string(existingPassword)
		} else {
			password = rand.Text()
			passwordReused = false
		}

		secret.Data["username"] = []byte(username)
		secret.Data["password"] = []byte(password)

		return controllerutil.SetControllerReference(owner, secret, scheme)
	})
	if err != nil {
		return "", false, err
	}

	return password, passwordReused, nil
}

func addAccessFinalizerIfMissing(ctx context.Context, c client.Client, obj client.Object) error {
	if controllerutil.ContainsFinalizer(obj, accessResourceFinalizer) {
		return nil
	}

	controllerutil.AddFinalizer(obj, accessResourceFinalizer)
	return c.Update(ctx, obj)
}

func removeAccessFinalizerIfPresent(ctx context.Context, c client.Client, obj client.Object) error {
	if !controllerutil.ContainsFinalizer(obj, accessResourceFinalizer) {
		return nil
	}

	controllerutil.RemoveFinalizer(obj, accessResourceFinalizer)
	return c.Update(ctx, obj)
}

func setReconcileStatus[T client.Object](
	ctx context.Context,
	c client.Client,
	key types.NamespacedName,
	config reconcileStatusConfig[T],
	reconcileState accessv1.ReconcileState,
	reason,
	message string,
) error {
	if key.Name == "" || key.Namespace == "" {
		return nil
	}

	latest := config.newObject()
	if err := c.Get(ctx, key, latest); err != nil {
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

	conditions := config.conditions(latest)
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               config.conditionTypes.Ready,
		Status:             readyStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: latest.GetGeneration(),
	})
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               config.conditionTypes.Success,
		Status:             successStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: latest.GetGeneration(),
	})
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               config.conditionTypes.InProgress,
		Status:             inProgressStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: latest.GetGeneration(),
	})
	config.setLastLog(latest, message)
	config.setLastReconcileState(latest, reconcileState)

	return c.Status().Update(ctx, latest)
}

func hasSharedConnectionDetails(connection accessv1.ConnectionSpec) bool {
	return connection.Username != nil && connection.Password != nil &&
		connection.Host != nil && *connection.Host != "" &&
		connection.Port != nil
}

func getDirectConnectionDetails(
	ctx context.Context,
	c client.Client,
	connection accessv1.ConnectionSpec,
	namespace string,
) (ConnectionDetails, error) {
	username, err := resolveValueOrSecretRef(ctx, c, connection.Username, namespace)
	if err != nil {
		return ConnectionDetails{}, fmt.Errorf("failed to resolve username: %w", err)
	}

	password, err := resolveValueOrSecretRef(ctx, c, connection.Password, namespace)
	if err != nil {
		return ConnectionDetails{}, fmt.Errorf("failed to resolve password: %w", err)
	}

	details := ConnectionDetails{
		SharedConnectionDetails: SharedConnectionDetails{
			Username: username,
			Password: password,
			Host:     *connection.Host,
			Port:     fmt.Sprintf("%d", *connection.Port),
		},
		SSLMode: resolveSSLMode(connection.SSLMode),
	}

	if connection.Database != nil && *connection.Database != "" {
		details.Database = *connection.Database
	}

	return details, nil
}

func resolveValueOrSecretRef(ctx context.Context, c client.Client, ref *accessv1.SecretKeySelector, namespace string) (string, error) {
	if ref == nil {
		return "", fmt.Errorf("value or secret reference is nil")
	}

	if ref.Value != nil {
		return *ref.Value, nil
	}

	if ref.SecretRef != nil {
		var sec corev1.Secret
		if err := c.Get(ctx, types.NamespacedName{Name: ref.SecretRef.Name, Namespace: namespace}, &sec); err != nil {
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

func resolveConnectionSecretNamespace(ctx context.Context, c client.Client, resourceNamespace string,
	requestedNamespace *string, onMultiple controllerMultipleHandler) (string, error) {
	secretNamespace := resourceNamespace
	if requestedNamespace == nil {
		return secretNamespace, nil
	}

	requested := strings.TrimSpace(*requestedNamespace)
	if requested == "" {
		return secretNamespace, nil
	}

	if requested == resourceNamespace {
		return requested, nil
	}

	allowed, err := resolveExistingSecretNamespacePolicy(ctx, c, onMultiple)
	if err != nil {
		return "", err
	}
	if !allowed {
		return "", fmt.Errorf(
			"cross-namespace connection secret references are disabled: requested namespace %q from resource namespace %q",
			requested, resourceNamespace,
		)
	}

	return requested, nil
}

func resolveExistingSecretNamespacePolicy(
	ctx context.Context,
	c client.Client,
	onMultiple controllerMultipleHandler,
) (bool, error) {
	controllerObj, err := resolveSingletonController(ctx, c, onMultiple)
	if err != nil {
		return false, err
	}
	if controllerObj == nil {
		return false, nil
	}

	if !controllerObj.Spec.Settings.ExistingSecretNamespace {
		return false, nil
	}

	operatorNamespace, err := resolveOperatorNamespace(ctx, c)
	if err != nil {
		return false, err
	}
	if controllerObj.Namespace != operatorNamespace {
		return false, fmt.Errorf(
			"cross-namespace connection secret references are disabled: Controller resource %q must be created in the operator namespace %q, found in %q",
			controllerObj.Name,
			operatorNamespace,
			controllerObj.Namespace,
		)
	}

	return true, nil
}

func resolveControllerSettings(
	ctx context.Context,
	c client.Client,
	onMultiple controllerMultipleHandler,
) (accessv1.ControllerSettings, error) {
	controllerObj, err := resolveSingletonController(ctx, c, onMultiple)
	if err != nil {
		return accessv1.ControllerSettings{}, err
	}
	if controllerObj == nil {
		return accessv1.ControllerSettings{}, nil
	}

	return controllerObj.Spec.Settings, nil
}

func resolveSingletonController(
	ctx context.Context,
	c client.Client,
	onMultiple controllerMultipleHandler,
) (*accessv1.Controller, error) {
	var controllers accessv1.ControllerList
	if err := c.List(ctx, &controllers); err != nil {
		return nil, err
	}

	switch len(controllers.Items) {
	case 0:
		return nil, nil
	case 1:
		return &controllers.Items[0], nil
	default:
		message := fmt.Sprintf(
			"multiple Controller resources found (%d); exactly one is allowed cluster-wide",
			len(controllers.Items),
		)
		if onMultiple != nil {
			for i := range controllers.Items {
				onMultiple(&controllers.Items[i], message)
			}
		}
		return nil, errors.New(message)
	}
}

func resolveOperatorNamespace(ctx context.Context, c client.Client) (string, error) {
	if podNamespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); podNamespace != "" {
		return podNamespace, nil
	}

	managerDeployments, err := listManagerDeployments(ctx, c)
	if err != nil {
		return "", err
	}

	switch len(managerDeployments) {
	case 0:
		return defaultManagerDeploymentNamespace, nil
	case 1:
		return managerDeployments[0].Namespace, nil
	default:
		sort.Slice(managerDeployments, func(i, j int) bool {
			if managerDeployments[i].Namespace == managerDeployments[j].Namespace {
				return managerDeployments[i].Name < managerDeployments[j].Name
			}
			return managerDeployments[i].Namespace < managerDeployments[j].Namespace
		})
		return managerDeployments[0].Namespace, nil
	}
}

func listManagerDeployments(ctx context.Context, c client.Client) ([]appsv1.Deployment, error) {
	var deploymentList appsv1.DeploymentList
	if err := c.List(
		ctx,
		&deploymentList,
		client.MatchingLabels{
			managerControlPlaneLabelKey: managerControlPlaneLabelValue,
			managerAppNameLabelKey:      managerAppNameLabelValue,
		},
	); err != nil {
		return nil, err
	}

	return deploymentList.Items, nil
}

func emitEvent(recorder events.EventRecorder, object client.Object, eventType, reason, message string) {
	if recorder == nil || object == nil {
		return
	}

	message = fmt.Sprintf("%s (at %s)", message, time.Now().Format(time.RFC3339))
	recorder.Eventf(object, nil, eventType, reason, "PolicyValidation", "%s", message)
}

func getExistingSecretConnectionDetails(
	ctx context.Context,
	c client.Client,
	secretName,
	namespace string,
	connection *accessv1.ConnectionSpec,
) (ConnectionDetails, error) {
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
		existingDatabase = defaultPostgresDatabase
	}

	if connection != nil && connection.Database != nil && *connection.Database != "" {
		existingDatabase = *connection.Database
	}

	return ConnectionDetails{
		SharedConnectionDetails: SharedConnectionDetails{
			Username: existingUsername,
			Password: existingPassword,
			Host:     existingHost,
			Port:     existingPort,
		},
		Database: existingDatabase,
		SSLMode:  resolveSSLModeFromSecret(existingSec.Data),
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
