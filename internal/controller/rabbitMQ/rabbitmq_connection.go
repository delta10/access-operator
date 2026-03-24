package rabbitMQ

import (
	"context"
	"fmt"
	"net"
	neturl "net/url"
	"strings"

	accessv1 "github.com/delta10/access-operator/api/v1"
	"github.com/delta10/access-operator/internal/controller"
	rabbithole "github.com/michaelklishin/rabbit-hole/v3"
	corev1 "k8s.io/api/core/v1"
)

const defaultRabbitMQAMQPPort = "5672"
const defaultRabbitMQManagementPort = "15672"

var connectionDefaults = accessv1.ConnectionSpec{}

func (r *AccessReconciler) initializeRabbitMQClientConnection(ctx context.Context, rbq *accessv1.RabbitMQAccess) (*rabbithole.Client, error) {
	connection, err := r.getConnectionDetails(ctx, rbq)
	if err != nil {
		return nil, err
	}

	endpoint := rabbitMQManagementEndpoint(connection.Host, connection.Port)
	return rabbithole.NewClient(endpoint, connection.Username, connection.Password)
}

func (r *AccessReconciler) getConnectionDetails(ctx context.Context, rbq *accessv1.RabbitMQAccess) (controller.ConnectionDetails, error) {
	if rbq.Spec.Connection.ExistingSecret != nil && *rbq.Spec.Connection.ExistingSecret != "" {
		secretNamespace, err := r.resolveExistingSecretNamespace(ctx, rbq)
		if err != nil {
			return controller.ConnectionDetails{}, err
		}

		return controller.GetExistingSecretConnectionDetails(
			ctx,
			r.Client,
			*rbq.Spec.Connection.ExistingSecret,
			secretNamespace,
			&rbq.Spec.Connection,
			connectionDefaults,
		)
	}

	if controller.HasDirectConnectionDetails(rbq.Spec.Connection) {
		return controller.GetDirectConnectionDetails(ctx, r.Client, rbq.Spec.Connection, rbq.Namespace, connectionDefaults)
	}

	return controller.ConnectionDetails{}, fmt.Errorf("no valid connection details provided")
}

// with interface here so we can use the emit function
func (r *AccessReconciler) resolveExistingSecretNamespace(
	ctx context.Context,
	rbq *accessv1.RabbitMQAccess,
) (string, error) {
	secretNamespace, err := controller.ResolveConnectionSecretNamespace(
		ctx,
		r.Client,
		rbq.Namespace,
		rbq.Spec.Connection.ExistingSecretNamespace,
		func(controllerObj *accessv1.Controller, message string) {
			controller.EmitEvent(r.Recorder, controllerObj, corev1.EventTypeWarning, controller.MultipleControllersFoundReason, message)
		},
	)
	if err != nil {
		if strings.Contains(err.Error(), "multiple Controller resources found") {
			controller.EmitEvent(r.Recorder, rbq, corev1.EventTypeWarning, controller.MultipleControllersFoundReason, err.Error())
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

func (r *AccessReconciler) resolveExcludedUsers(ctx context.Context) (map[string]struct{}, error) {
	settings, err := resolveRabbitMQControllerSettings(ctx, r)
	if err != nil {
		return nil, err
	}

	return controller.NormalizeExcludedUsers(settings.RabbitMQSettings.ExcludedUsers), nil
}

func (r *AccessReconciler) resolveExcludedVhosts(ctx context.Context) (map[string]struct{}, error) {
	settings, err := resolveRabbitMQControllerSettings(ctx, r)
	if err != nil {
		return nil, err
	}

	excludedVhosts := controller.NormalizeExcludedUsers(settings.RabbitMQSettings.ExcludedVhosts)
	excludedVhosts["/"] = struct{}{}

	return excludedVhosts, nil
}

func (r *AccessReconciler) resolveStaleVhostDeletionPolicy(ctx context.Context) (accessv1.StaleVhostDeletionPolicy, error) {
	settings, err := resolveRabbitMQControllerSettings(ctx, r)
	if err != nil {
		return "", err
	}

	if settings.RabbitMQSettings.StaleVhostDeletionPolicy == nil {
		return accessv1.StaleVhostDeletionPolicyRetain, nil
	}

	return *settings.RabbitMQSettings.StaleVhostDeletionPolicy, nil
}
