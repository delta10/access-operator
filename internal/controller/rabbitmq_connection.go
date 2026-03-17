package controller

import (
	"context"
	"fmt"
	"net"
	neturl "net/url"
	"strings"

	accessv1 "github.com/delta10/access-operator/api/v1"
	rabbithole "github.com/michaelklishin/rabbit-hole/v3"
	corev1 "k8s.io/api/core/v1"
)

const defaultRabbitMQAMQPPort = "5672"
const defaultRabbitMQManagementPort = "15672"

func (r *RabbitMQAccessReconciler) initializeRabbitMQClientConnection(ctx context.Context, rbq *accessv1.RabbitMQAccess) (*rabbithole.Client, error) {
	connection, err := r.getConnectionDetails(ctx, rbq)
	if err != nil {
		return nil, err
	}

	endpoint := rabbitMQManagementEndpoint(connection.Host, connection.Port)
	return rabbithole.NewClient(endpoint, connection.Username, connection.Password)
}

func (r *RabbitMQAccessReconciler) getConnectionDetails(ctx context.Context, rbq *accessv1.RabbitMQAccess) (ConnectionDetails, error) {
	if rbq.Spec.Connection.ExistingSecret != nil && *rbq.Spec.Connection.ExistingSecret != "" {
		secretNamespace, err := r.resolveExistingSecretNamespace(ctx, rbq)
		if err != nil {
			return ConnectionDetails{}, err
		}

		return getExistingSecretConnectionDetails(
			ctx,
			r.Client,
			*rbq.Spec.Connection.ExistingSecret,
			secretNamespace,
			&rbq.Spec.Connection,
		)
	}

	if hasSharedConnectionDetails(rbq.Spec.Connection) {
		return getDirectConnectionDetails(ctx, r.Client, rbq.Spec.Connection, rbq.Namespace)
	}

	return ConnectionDetails{}, fmt.Errorf("no valid connection details provided")
}

// with interface here so we can use the emit function
func (r *RabbitMQAccessReconciler) resolveExistingSecretNamespace(
	ctx context.Context,
	rbq *accessv1.RabbitMQAccess,
) (string, error) {
	secretNamespace, err := resolveConnectionSecretNamespace(
		ctx,
		r.Client,
		rbq.Namespace,
		rbq.Spec.Connection.ExistingSecretNamespace,
		func(controllerObj *accessv1.Controller, message string) {
			emitEvent(r.Recorder, controllerObj, corev1.EventTypeWarning, multipleControllersFoundReason, message)
		},
	)
	if err != nil {
		if strings.Contains(err.Error(), "multiple Controller resources found") {
			emitEvent(r.Recorder, rbq, corev1.EventTypeWarning, multipleControllersFoundReason, err.Error())
		}
		return "", err
	}

	return secretNamespace, nil
}

func rabbitMQManagementEndpoint(host, port string) string {
	managementPort := strings.TrimSpace(port)
	if managementPort == "" || managementPort == defaultRabbitMQAMQPPort {
		managementPort = defaultRabbitMQManagementPort
	}

	if strings.HasPrefix(host, "http://") || strings.HasPrefix(host, "https://") {
		parsed, err := neturl.Parse(host)
		if err == nil {
			if parsed.Port() == "" {
				parsed.Host = net.JoinHostPort(parsed.Hostname(), managementPort)
			}
			parsed.Path = ""
			parsed.RawPath = ""
			parsed.RawQuery = ""
			parsed.Fragment = ""
			return strings.TrimRight(parsed.String(), "/")
		}
	}

	return fmt.Sprintf("http://%s:%s", host, managementPort)
}

func (r *RabbitMQAccessReconciler) resolveExcludedUsers(ctx context.Context) (map[string]struct{}, error) {
	settings, err := resolveRabbitMQControllerSettings(ctx, r)
	if err != nil {
		return nil, err
	}

	return normalizeExcludedUsers(settings.RabbitMQSettings.ExcludedUsers), nil
}

func (r *RabbitMQAccessReconciler) resolveExcludedVhosts(ctx context.Context) (map[string]struct{}, error) {
	settings, err := resolveRabbitMQControllerSettings(ctx, r)
	if err != nil {
		return nil, err
	}

	excludedVhosts := normalizeExcludedUsers(settings.RabbitMQSettings.ExcludedVhosts)
	excludedVhosts["/"] = struct{}{}

	return excludedVhosts, nil
}

func (r *RabbitMQAccessReconciler) resolveStaleVhostDeletionPolicy(ctx context.Context) (accessv1.StaleVhostDeletionPolicy, error) {
	settings, err := resolveRabbitMQControllerSettings(ctx, r)
	if err != nil {
		return "", err
	}

	if settings.RabbitMQSettings.StaleVhostDeletionPolicy == nil {
		return accessv1.StaleVhostDeletionPolicyRetain, nil
	}

	return *settings.RabbitMQSettings.StaleVhostDeletionPolicy, nil
}
