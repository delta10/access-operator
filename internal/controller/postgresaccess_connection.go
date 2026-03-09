package controller

import (
	"context"
	"fmt"
	"strings"

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

		connection, err := getExistingSecretConnectionDetails(ctx, r.Client, *pg.Spec.Connection.ExistingSecret, secretNamespace, &pg.Spec.Connection)
		if err != nil {
			return "", err
		}
		return formatConnectionString(connection), nil
	}

	c := pg.Spec.Connection
	if hasDirectConnectionDetails(c) {
		connection, err := getDirectConnectionDetails(ctx, r.Client, c, pg.Namespace)
		if err != nil {
			return "", err
		}

		return formatConnectionString(connection), nil
	}

	return "", fmt.Errorf("no valid connection details provided")
}

func hasDirectConnectionDetails(c accessv1.ConnectionSpec) bool {
	return hasSharedConnectionDetails(c) && c.Database != nil && *c.Database != ""
}

func (r *PostgresAccessReconciler) resolveExistingSecretNamespace(ctx context.Context, pg *accessv1.PostgresAccess) (string, error) {
	secretNamespace, err := resolveConnectionSecretNamespace(
		ctx,
		r.Client,
		pg.Namespace,
		pg.Spec.Connection.ExistingSecretNamespace,
		func(controllerObj *accessv1.Controller, message string) {
			r.emitEvent(controllerObj, "Warning", multipleControllersFoundReason, message)
		},
	)
	if err != nil {
		if strings.Contains(err.Error(), "multiple Controller resources found") {
			r.emitEvent(pg, "Warning", multipleControllersFoundReason, err.Error())
		}
		return "", err
	}

	return secretNamespace, nil
}

func (r *PostgresAccessReconciler) resolveExistingSecretNamespacePolicy(ctx context.Context) (bool, error) {
	settings, err := resolvePostgresControllerSettings(ctx, r)
	if err != nil {
		return false, err
	}

	return settings.ExistingSecretNamespace, nil
}

func (r *PostgresAccessReconciler) resolveExcludedUsers(ctx context.Context) (map[string]struct{}, error) {
	settings, err := resolvePostgresControllerSettings(ctx, r)
	if err != nil {
		return nil, err
	}

	return normalizeExcludedUsers(settings.PostgresSettings.ExcludedUsers), nil
}

func (r *PostgresAccessReconciler) resolveControllerSettings(ctx context.Context) (accessv1.ControllerSettings, error) {
	return resolvePostgresControllerSettings(ctx, r)
}

func normalizeExcludedUsers(users []string) map[string]struct{} {
	normalized := make(map[string]struct{}, len(users))
	for _, user := range users {
		trimmed := strings.TrimSpace(user)
		if trimmed == "" {
			continue
		}
		normalized[trimmed] = struct{}{}
	}

	return normalized
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
