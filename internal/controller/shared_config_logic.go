package controller

import (
	"context"
	"fmt"
	"strings"

	accessv1 "github.com/delta10/access-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type SharedControllerMultipleHandler func(*accessv1.Controller, string)

type SharedConnectionDetails struct {
	Username string
	Password string
	Host     string
	Port     string
}

type ConnectionDetails struct {
	SharedConnectionDetails
	Database string
	SSLMode  string
}

func HasDirectConnectionDetails(connection accessv1.ConnectionSpec) bool {
	return connection.Username != nil && connection.Password != nil &&
		connection.Host != nil && *connection.Host != "" &&
		connection.Port != nil
}

func GetDirectConnectionDetails(
	ctx context.Context,
	c client.Client,
	connection accessv1.ConnectionSpec,
	namespace string,
	defaults accessv1.ConnectionSpec,
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
		Database: resolveDatabase(connection.Database, defaults),
		SSLMode:  resolveSSLMode(connection.SSLMode, defaults),
	}

	return details, nil
}

func ResolveConnectionSecretNamespace(
	ctx context.Context,
	c client.Client,
	resourceNamespace string,
	requestedNamespace *string,
	onMultiple SharedControllerMultipleHandler,
) (string, error) {
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

func ResolveControllerSettings(
	ctx context.Context,
	c client.Client,
	onMultiple SharedControllerMultipleHandler,
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

func ListManagerDeployments(ctx context.Context, c client.Client) ([]appsv1.Deployment, error) {
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

func NormalizeExcludedUsers(users []string) map[string]struct{} {
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
