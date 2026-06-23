package postgres

import (
	"context"
	"fmt"
	"net"
	"net/url"

	accessv1 "github.com/delta10/access-operator/api/v1"
	"github.com/delta10/access-operator/internal/controller"
)

var defaultPort = int32(5432)
var defaultDatabase = "postgres"
var defaultSSLMode = "require"
var connectionDefaults = accessv1.ConnectionSpec{
	Port:     &defaultPort,
	Database: &defaultDatabase,
	SSLMode:  &defaultSSLMode,
}

// getConnectionString constructs the PostgreSQL connection string based on the PostgresAccess spec.
// It supports both direct connection details and referencing an existing secret for connection information.
func (r *PostgresAccessReconciler) getConnectionString(ctx context.Context, pg *accessv1.PostgresAccess) (string, error) {
	if pg.Spec.Connection.ExistingSecret != nil && *pg.Spec.Connection.ExistingSecret != "" {
		secretNamespace, err := r.resolveExistingSecretNamespace(ctx, pg)
		if err != nil {
			return "", err
		}

		connection, err := controller.GetExistingSecretConnectionDetails(ctx, r.Client, *pg.Spec.Connection.ExistingSecret, secretNamespace, &pg.Spec.Connection, connectionDefaults)
		if err != nil {
			return "", err
		}
		return formatConnectionString(connection), nil
	}

	c := pg.Spec.Connection
	if hasDirectConnectionDetails(c) {
		secretNamespace, err := r.resolveExistingSecretNamespace(ctx, pg)
		if err != nil {
			return "", err
		}
		connection, err := controller.GetDirectConnectionDetails(ctx, r.Client, c, secretNamespace, connectionDefaults)
		if err != nil {
			return "", err
		}

		return formatConnectionString(connection), nil
	}

	return "", fmt.Errorf("no valid connection details provided")
}

func hasDirectConnectionDetails(c accessv1.ConnectionSpec) bool {
	return controller.HasDirectConnectionDetails(c)
}

func (r *PostgresAccessReconciler) resolveExistingSecretNamespace(ctx context.Context, pg *accessv1.PostgresAccess) (string, error) {
	secretNamespace, err := controller.ResolveConnectionSecretNamespace(
		ctx,
		r.Client,
		pg.Namespace,
		pg.Spec.Connection.ExistingSecretNamespace,
	)
	if err != nil {
		return "", err
	}

	return secretNamespace, nil
}

func (r *PostgresAccessReconciler) resolveExcludedUsers(ctx context.Context) (map[string]struct{}, error) {
	settings, err := resolvePostgresControllerSettings(ctx, r)
	if err != nil {
		return nil, err
	}

	return controller.NormalizeExcludedUsers(settings.PostgresSettings.ExcludedUsers), nil
}

func (r *PostgresAccessReconciler) resolveStaleUserDeletionPolicy(ctx context.Context) (accessv1.PostgresCleanupPolicy, error) {
	if r.Client == nil {
		return accessv1.CleanupPolicyRestrict, nil
	}

	settings, err := resolvePostgresControllerSettings(ctx, r)
	if err != nil {
		return "", err
	}

	if settings.PostgresSettings.StaleUserDeletionPolicy == nil {
		return accessv1.CleanupPolicyRestrict, nil
	}

	return *settings.PostgresSettings.StaleUserDeletionPolicy, nil
}

func formatConnectionString(connection controller.ConnectionDetails) string {
	connectionURL := url.URL{
		Scheme: "postgresql",
		User:   url.UserPassword(connection.Username, connection.Password),
		Host:   net.JoinHostPort(connection.Host, connection.Port),
		Path:   "/" + connection.Database,
	}
	query := connectionURL.Query()
	query.Set("sslmode", connection.SSLMode)
	connectionURL.RawQuery = query.Encode()

	return connectionURL.String()
}
