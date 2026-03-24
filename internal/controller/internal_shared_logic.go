package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	neturl "net/url"
	"os"
	"sort"
	"strings"

	accessv1 "github.com/delta10/access-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func resolveValueOrSecretRef(
	ctx context.Context,
	c client.Client,
	ref *accessv1.SecretKeySelector,
	namespace string,
) (string, error) {
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

func resolveExistingSecretNamespacePolicy(
	ctx context.Context,
	c client.Client,
	onMultiple SharedControllerMultipleHandler,
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

func resolveSingletonController(
	ctx context.Context,
	c client.Client,
	onMultiple SharedControllerMultipleHandler,
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

	managerDeployments, err := ListManagerDeployments(ctx, c)
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

func resolveSSLMode(sslMode *string, defaults accessv1.ConnectionSpec) string {
	if sslMode != nil && *sslMode != "" {
		return *sslMode
	}

	if defaults.SSLMode != nil && *defaults.SSLMode != "" {
		return *defaults.SSLMode
	}

	return ""
}

func resolveSSLModeFromSecret(secretData map[string][]byte, defaults accessv1.ConnectionSpec) string {
	if existingSSLMode, ok := getSecretValue(secretData, "sslmode"); ok {
		return existingSSLMode
	}

	if defaults.SSLMode != nil && *defaults.SSLMode != "" {
		return *defaults.SSLMode
	}
	return ""
}

func resolveDatabase(database *string, defaults accessv1.ConnectionSpec) string {
	if database != nil {
		trimmedDatabase := strings.TrimSpace(*database)
		switch strings.ToLower(trimmedDatabase) {
		case "", "*", "%", "(none)", "null":
		default:
			return trimmedDatabase
		}
	}

	if defaults.Database != nil && *defaults.Database != "" {
		return *defaults.Database
	}

	return ""
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
