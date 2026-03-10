//go:build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/gomega" // nolint:revive,staticcheck

	accessv1 "github.com/delta10/access-operator/api/v1"
	"github.com/delta10/access-operator/internal/controller"
)

// GetRabbitMQVariables returns the namespace and admin connection details for e2e RabbitMQ tests.
func GetRabbitMQVariables() (string, controller.ConnectionDetails) {
	testNamespace := os.Getenv("RABBITMQ_TEST_NAMESPACE")
	if testNamespace == "" {
		testNamespace = "rabbitmq-access-test"
	}
	defaultClusterHost := fmt.Sprintf("rabbitmq.%s.svc", testNamespace)

	host := os.Getenv("RABBITMQ_CLUSTER_HOST")
	if host == "" {
		host = os.Getenv("RABBITMQ_HOST")
	}
	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "rabbitmq" {
		host = defaultClusterHost
	}

	port := os.Getenv("RABBITMQ_CLUSTER_PORT")
	if port == "" {
		port = os.Getenv("RABBITMQ_PORT")
	}
	if port == "" {
		port = "5672"
	}

	username := os.Getenv("RABBITMQ_USER")
	if username == "" {
		username = "admin"
	}

	password := os.Getenv("RABBITMQ_PASSWORD")
	if password == "" {
		password = "secret"
	}

	return testNamespace, controller.ConnectionDetails{
		SharedConnectionDetails: controller.SharedConnectionDetails{
			Host:     host,
			Port:     port,
			Username: username,
			Password: password,
		},
	}
}

// DeployRabbitMQInstance deploys a RabbitMQ service/deployment into the provided namespace.
func DeployRabbitMQInstance(namespace string, connection controller.ConnectionDetails) error {
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Service
metadata:
  name: rabbitmq
  namespace: %s
spec:
  selector:
    app: rabbitmq
  ports:
    - name: amqp
      port: 5672
      targetPort: 5672
    - name: management
      port: 15672
      targetPort: 15672
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rabbitmq
  namespace: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: rabbitmq
  template:
    metadata:
      labels:
        app: rabbitmq
    spec:
      containers:
        - name: rabbitmq
          image: rabbitmq:3.13-management-alpine
          env:
            - name: RABBITMQ_DEFAULT_USER
              value: %q
            - name: RABBITMQ_DEFAULT_PASS
              value: %q
          ports:
            - containerPort: 5672
            - containerPort: 15672
`, namespace, namespace, connection.Username, connection.Password)

	return ApplyManifest(manifest)
}

func DeleteRabbitMQInstance(namespace string) error {
	cmd := exec.Command("kubectl", "delete", "deployment,service", "rabbitmq", "-n", namespace, "--ignore-not-found")
	_, err := Run(cmd)
	return err
}

// WaitForRabbitMQReady waits for the RabbitMQ deployment to become available and respond to diagnostics.
func WaitForRabbitMQReady(namespace string) {
	cmd := exec.Command(
		"kubectl",
		"wait",
		"--for=condition=Available",
		"deployment/rabbitmq",
		"-n",
		namespace,
		"--timeout=2m",
	)
	_, err := Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "RabbitMQ deployment should become available")

	Eventually(func(g Gomega) {
		output, runErr := runRabbitMQCommand(namespace, "rabbitmq-diagnostics", "-q", "ping")
		g.Expect(runErr).NotTo(HaveOccurred(), "RabbitMQ should respond to diagnostics")
		g.Expect(strings.TrimSpace(output)).To(ContainSubstring("Ping succeeded"))
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// RunRabbitMQctl executes rabbitmqctl in the in-cluster RabbitMQ pod.
func RunRabbitMQctl(namespace string, args ...string) (string, error) {
	return runRabbitMQCommand(namespace, "rabbitmqctl", args...)
}

// CreateRabbitMQConnectionDetailsViaSecret creates a connection secret used by RabbitMQAccess.
func CreateRabbitMQConnectionDetailsViaSecret(namespace string, connection controller.ConnectionDetails) (string, error) {
	secretName := "rabbitmq-connection-secret"
	secretYAML := fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
type: Opaque
stringData:
  host: %q
  port: %q
  username: %q
  password: %q
`, secretName, namespace, connection.Host, connection.Port, connection.Username, connection.Password)

	return secretName, ApplyManifest(secretYAML)
}

// CreateRabbitMQAccessWithDirectConnection creates a RabbitMQAccess with inline connection fields.
func CreateRabbitMQAccessWithDirectConnection(
	username,
	namespace,
	generatedSecretName string,
	connection controller.ConnectionDetails,
	permissions []accessv1.RabbitMQPermissionSpec,
) error {
	rabbitYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: RabbitMQAccess
metadata:
  name: %s
  namespace: %s
spec:
  generatedSecret: %s
  username: %s
  connection:
    host: %s
    port: %s
    username:
      value: %s
    password:
      value: %s
  permissions:
%s
`, username, namespace, generatedSecretName, username, connection.Host, connection.Port, connection.Username, connection.Password, formatRabbitMQPermissionsYAML(permissions, "    "))

	return ApplyManifest(rabbitYAML)
}

// CreateRabbitMQAccessWithConnectionSecretRef creates a RabbitMQAccess with direct host/port and secret-ref credentials.
func CreateRabbitMQAccessWithConnectionSecretRef(
	username,
	namespace,
	generatedSecretName string,
	connection controller.ConnectionDetails,
	secretName string,
	permissions []accessv1.RabbitMQPermissionSpec,
) error {
	rabbitYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: RabbitMQAccess
metadata:
  name: %s
  namespace: %s
spec:
  generatedSecret: %s
  username: %s
  connection:
    host: %s
    port: %s
    username:
      secretRef:
        name: %s
        key: username
    password:
      secretRef:
        name: %s
        key: password
  permissions:
%s
`, username, namespace, generatedSecretName, username, connection.Host, connection.Port, secretName, secretName, formatRabbitMQPermissionsYAML(permissions, "    "))

	return ApplyManifest(rabbitYAML)
}

// CreateRabbitMQAccessFromSecretReference creates a RabbitMQAccess referencing an existing connection secret.
func CreateRabbitMQAccessFromSecretReference(
	username,
	namespace,
	generatedSecretName,
	connSecret string,
	connSecretNamespace *string,
	permissions []accessv1.RabbitMQPermissionSpec,
) error {
	existingSecretNamespaceYAML := ""
	if connSecretNamespace != nil && strings.TrimSpace(*connSecretNamespace) != "" {
		existingSecretNamespaceYAML = fmt.Sprintf("    existingSecretNamespace: %s\n", strings.TrimSpace(*connSecretNamespace))
	}

	rabbitYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: RabbitMQAccess
metadata:
  name: %s
  namespace: %s
spec:
  generatedSecret: %s
  username: %s
  connection:
    existingSecret: %s
%s
  permissions:
%s
`, username, namespace, generatedSecretName, username, connSecret, existingSecretNamespaceYAML, formatRabbitMQPermissionsYAML(permissions, "    "))

	return ApplyManifest(rabbitYAML)
}

// WaitForRabbitMQUserState waits until a RabbitMQ user either exists or is removed.
func WaitForRabbitMQUserState(namespace, username string, shouldExist bool) {
	Eventually(func(g Gomega) {
		output, err := RunRabbitMQctl(namespace, "list_users")
		g.Expect(err).NotTo(HaveOccurred(), "Failed to list RabbitMQ users")
		g.Expect(rabbitMQOutputContainsFirstField(output, username)).To(Equal(shouldExist))
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// WaitForRabbitMQVhostState waits until a RabbitMQ vhost either exists or is removed.
func WaitForRabbitMQVhostState(namespace, vhost string, shouldExist bool) {
	Eventually(func(g Gomega) {
		output, err := RunRabbitMQctl(namespace, "list_vhosts")
		g.Expect(err).NotTo(HaveOccurred(), "Failed to list RabbitMQ vhosts")
		g.Expect(rabbitMQOutputContainsFirstField(output, vhost)).To(Equal(shouldExist))
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// WaitForRabbitMQPermissions waits until all expected permissions are present for the user.
func WaitForRabbitMQPermissions(namespace, username string, expected []accessv1.RabbitMQPermissionSpec) {
	Eventually(func(g Gomega) {
		output, err := RunRabbitMQctl(namespace, "list_user_permissions", username)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to list RabbitMQ user permissions")
		for _, permission := range expected {
			g.Expect(rabbitMQPermissionsContain(output, permission)).To(BeTrue(),
				"expected RabbitMQ permissions for user %s to include vhost %s", username, permission.VHost)
		}
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// WaitForRabbitMQAuthenticationSuccess waits until RabbitMQ accepts the provided credentials.
func WaitForRabbitMQAuthenticationSuccess(namespace, username, password string) {
	Eventually(func(g Gomega) {
		_, err := RunRabbitMQctl(namespace, "authenticate_user", username, password)
		g.Expect(err).NotTo(HaveOccurred(), "RabbitMQ should accept the expected credentials")
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// DeleteRabbitMQAccess deletes a RabbitMQAccess resource.
func DeleteRabbitMQAccess(name, namespace string) error {
	cmd := exec.Command("kubectl", "delete", "rabbitmqaccess", name, "-n", namespace)
	_, err := Run(cmd)
	return err
}

func runRabbitMQCommand(namespace, command string, args ...string) (string, error) {
	podName, err := getRabbitMQPodName(namespace)
	if err != nil {
		return "", err
	}

	cmdArgs := []string{
		"exec",
		"-n",
		namespace,
		"pod/" + podName,
		"-c",
		"rabbitmq",
		"--",
		command,
	}
	cmdArgs = append(cmdArgs, args...)

	cmd := exec.Command("kubectl", cmdArgs...)
	output, err := Run(cmd)
	return strings.TrimSpace(output), err
}

func getRabbitMQPodName(namespace string) (string, error) {
	cmd := exec.Command(
		"kubectl",
		"get",
		"pods",
		"-n",
		namespace,
		"-l",
		"app=rabbitmq",
		"--field-selector=status.phase=Running",
		"-o",
		"jsonpath={.items[0].metadata.name}",
	)
	output, err := Run(cmd)
	if err != nil {
		return "", err
	}

	podName := strings.TrimSpace(output)
	if podName == "" {
		return "", fmt.Errorf("failed to find RabbitMQ pod in namespace %q", namespace)
	}

	cmd = exec.Command("kubectl", "wait", "--for=condition=Ready", "pod/"+podName, "-n", namespace, "--timeout=30s")
	_, err = Run(cmd)
	if err != nil {
		return "", fmt.Errorf("pod %q is not ready for RabbitMQ exec: %w", podName, err)
	}

	return podName, nil
}

func formatRabbitMQPermissionsYAML(permissions []accessv1.RabbitMQPermissionSpec, indent string) string {
	lines := make([]string, 0, len(permissions)*4)
	for _, permission := range permissions {
		lines = append(lines,
			fmt.Sprintf("%s- vhost: %s", indent, permission.VHost),
			fmt.Sprintf("%s  configure: %q", indent, permission.Configure),
			fmt.Sprintf("%s  write: %q", indent, permission.Write),
			fmt.Sprintf("%s  read: %q", indent, permission.Read),
		)
	}
	return strings.Join(lines, "\n")
}

func rabbitMQOutputContainsFirstField(output, expected string) bool {
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) == 0 {
			continue
		}
		if fields[0] == expected {
			return true
		}
	}
	return false
}

func rabbitMQPermissionsContain(output string, permission accessv1.RabbitMQPermissionSpec) bool {
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 4 {
			continue
		}
		if fields[0] == permission.VHost &&
			fields[1] == permission.Configure &&
			fields[2] == permission.Write &&
			fields[3] == permission.Read {
			return true
		}
	}
	return false
}
