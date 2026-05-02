package controller

import (
	"context"
	"crypto/rand"
	"fmt"
	"slices"
	"strings"

	accessv1 "github.com/delta10/access-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func ReconcileGeneratedCredentialsSecret(
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

func GetExistingSecretConnectionDetails(
	ctx context.Context,
	c client.Client,
	secretName,
	namespace string,
	connection *accessv1.ConnectionSpec,
	defaults accessv1.ConnectionSpec,
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
		if defaults.Database != nil && *defaults.Database != "" {
			existingDatabase = *defaults.Database
		}
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
		SSLMode:  resolveSSLModeFromSecret(existingSec.Data, defaults),
	}, nil
}
