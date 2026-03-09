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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

type controllerMultipleHandler func(*accessv1.Controller, string)

type SharedConnectionDetails struct {
	Username string
	Password string
	Host     string
	Port     string
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
	settings, err := resolveControllerSettings(ctx, c, onMultiple)
	if err != nil {
		return false, err
	}

	return settings.ExistingSecretNamespace, nil
}

func resolveControllerSettings(
	ctx context.Context,
	c client.Client,
	onMultiple controllerMultipleHandler,
) (accessv1.ControllerSettings, error) {
	var controllers accessv1.ControllerList
	if err := c.List(ctx, &controllers); err != nil {
		return accessv1.ControllerSettings{}, err
	}

	switch len(controllers.Items) {
	case 0:
		return accessv1.ControllerSettings{}, nil
	case 1:
		return controllers.Items[0].Spec.Settings, nil
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
		return accessv1.ControllerSettings{}, errors.New(message)
	}
}

func resolvePostgresControllerSettings(ctx context.Context, r *PostgresAccessReconciler) (accessv1.ControllerSettings, error) {
	return resolveControllerSettings(ctx, r.Client, func(controllerObj *accessv1.Controller, message string) {
		r.emitEvent(controllerObj, "Warning", multipleControllersFoundReason, message)
	})
}

func resolveRabbitMQControllerSettings(ctx context.Context, r *RabbitMQAccessReconciler) (accessv1.ControllerSettings, error) {
	return resolveControllerSettings(ctx, r.Client, nil)
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
