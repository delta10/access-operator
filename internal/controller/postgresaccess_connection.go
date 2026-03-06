package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	neturl "net/url"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

const defaultPostgresDatabase = "postgres"
const defaultPostgresSSLMode = "require"

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
		return formatConnectionString(connection), nil
	}

	c := pg.Spec.Connection
	if hasDirectConnectionDetails(c) {
		connection, err := r.getDirectConnectionDetails(ctx, c, pg.Namespace)
		if err != nil {
			return "", err
		}

		return formatConnectionString(connection), nil
	}

	return "", fmt.Errorf("no valid connection details provided")
}

func hasDirectConnectionDetails(c accessv1.ConnectionSpec) bool {
	return c.Username != nil && c.Password != nil &&
		c.Host != nil && *c.Host != "" &&
		c.Port != nil &&
		c.Database != nil && *c.Database != ""
}

func (r *PostgresAccessReconciler) getDirectConnectionDetails(
	ctx context.Context,
	connection accessv1.ConnectionSpec,
	namespace string,
) (ConnectionDetails, error) {
	username, err := r.resolveValueOrSecretRef(ctx, connection.Username, namespace)
	if err != nil {
		return ConnectionDetails{}, fmt.Errorf("failed to resolve username: %w", err)
	}

	password, err := r.resolveValueOrSecretRef(ctx, connection.Password, namespace)
	if err != nil {
		return ConnectionDetails{}, fmt.Errorf("failed to resolve password: %w", err)
	}

	return ConnectionDetails{
		Username: username,
		Password: password,
		Host:     *connection.Host,
		Port:     fmt.Sprintf("%d", *connection.Port),
		Database: *connection.Database,
		SSLMode:  resolveSSLMode(connection.SSLMode),
	}, nil
}

// resolveValueOrSecretRef resolves a value that can be either a direct value or a secret reference.
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
		existingDatabase = defaultPostgresDatabase
	}

	// if a database is specified, it's more important than the existing secret
	if pg != nil {
		conn := pg.Spec.Connection
		if conn.Database != nil && *conn.Database != "" {
			existingDatabase = *conn.Database
		}
	}

	return ConnectionDetails{
		Username: existingUsername,
		Password: existingPassword,
		Host:     existingHost,
		Port:     existingPort,
		Database: existingDatabase,
		SSLMode:  resolveSSLModeFromSecret(existingSec.Data),
	}, nil
}

func formatConnectionString(connection ConnectionDetails) string {
	return fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s?sslmode=%s",
		connection.Username,
		connection.Password,
		connection.Host,
		connection.Port,
		connection.Database,
		connection.SSLMode,
	)
}

func resolveSSLMode(sslMode *string) string {
	if sslMode != nil && *sslMode != "" {
		return *sslMode
	}

	return defaultPostgresSSLMode
}

func resolveSSLModeFromSecret(secretData map[string][]byte) string {
	if existingSSLMode, ok := getSecretValue(secretData, "sslmode"); ok {
		return existingSSLMode
	}

	return defaultPostgresSSLMode
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
