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

	"github.com/delta10/access-operator/internal/controller"
)

// RedisConnectionDetailsForNamespace returns admin connection details for the provided Redis namespace.
func RedisConnectionDetailsForNamespace(testNamespace string) controller.ConnectionDetails {
	defaultClusterHost := fmt.Sprintf("redis.%s.svc", testNamespace)

	host := os.Getenv("REDIS_CLUSTER_HOST")
	if host == "" {
		host = os.Getenv("REDIS_HOST")
	}
	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "redis" {
		host = defaultClusterHost
	}

	port := os.Getenv("REDIS_CLUSTER_PORT")
	if port == "" {
		port = os.Getenv("REDIS_PORT")
	}
	if port == "" {
		port = "6379"
	}

	username := os.Getenv("REDIS_USER")
	if username == "" {
		username = "default"
	}

	password := os.Getenv("REDIS_PASSWORD")
	if password == "" {
		password = "secret"
	}

	return controller.ConnectionDetails{
		SharedConnectionDetails: controller.SharedConnectionDetails{
			Host:     host,
			Port:     port,
			Username: username,
			Password: password,
		},
	}
}

// GetRedisVariables returns the namespace and admin connection details for e2e Redis tests.
func GetRedisVariables() (string, controller.ConnectionDetails) {
	testNamespace := os.Getenv("REDIS_TEST_NAMESPACE")
	if testNamespace == "" {
		testNamespace = "redis-access-test"
	}

	return testNamespace, RedisConnectionDetailsForNamespace(testNamespace)
}

// DeployRedisInstance deploys a Redis service/deployment into the provided namespace.
func DeployRedisInstance(namespace string, connection controller.ConnectionDetails) error {
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Service
metadata:
  name: redis
  namespace: %s
spec:
  selector:
    app: redis
  ports:
    - name: redis
      port: 6379
      targetPort: 6379
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  namespace: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: redis:7.4-alpine
          imagePullPolicy: IfNotPresent
          args:
            - redis-server
            - --appendonly
            - "no"
            - --requirepass
            - %q
          ports:
            - containerPort: 6379
`, namespace, namespace, connection.Password)

	return ApplyManifest(manifest)
}

func DeleteRedisInstance(namespace string) error {
	cmd := exec.Command("kubectl", "delete", "deployment,service", "redis", "-n", namespace, "--ignore-not-found")
	_, err := Run(cmd)
	return err
}

// WaitForRedisReady waits for the Redis deployment to become available and respond to commands.
func WaitForRedisReady(namespace string, connection controller.ConnectionDetails) {
	cmd := exec.Command(
		"kubectl",
		"wait",
		"--for=condition=Available",
		"deployment/redis",
		"-n",
		namespace,
		"--timeout=2m",
	)
	_, err := Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Redis deployment should become available")

	Eventually(func(g Gomega) {
		output, runErr := RunRedisCLI(namespace, connection, "PING")
		g.Expect(runErr).NotTo(HaveOccurred(), "Redis should respond to ping")
		g.Expect(strings.TrimSpace(output)).To(Equal("PONG"))
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// RunRedisCLI executes redis-cli in the in-cluster Redis pod using the provided admin connection.
func RunRedisCLI(namespace string, connection controller.ConnectionDetails, args ...string) (string, error) {
	podName, err := getRedisPodName(namespace)
	if err != nil {
		return "", err
	}

	cmdArgs := []string{
		"exec",
		"-n",
		namespace,
		"pod/" + podName,
		"-c",
		"redis",
		"--",
		"redis-cli",
		"--raw",
		"--no-auth-warning",
		"--user",
		connection.Username,
		"-a",
		connection.Password,
	}
	cmdArgs = append(cmdArgs, args...)

	cmd := exec.Command("kubectl", cmdArgs...)
	output, err := Run(cmd)
	return strings.TrimSpace(output), err
}

// CreateRedisConnectionDetailsViaSecret creates a connection secret used by RedisAccess.
func CreateRedisConnectionDetailsViaSecret(namespace string, connection controller.ConnectionDetails) (string, error) {
	secretName := "redis-connection-secret"
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

// CreateRedisAccessWithDirectConnection creates a RedisAccess with inline connection fields.
func CreateRedisAccessWithDirectConnection(
	username,
	namespace,
	generatedSecretName string,
	connection controller.ConnectionDetails,
	aclRules []string,
) error {
	redisYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: RedisAccess
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
  aclRules:
%s
`, username, namespace, generatedSecretName, username, connection.Host, connection.Port, connection.Username, connection.Password, formatRedisACLRulesYAML(aclRules, "    "))

	return ApplyManifest(redisYAML)
}

// CreateRedisAccessWithConnectionSecretRef creates a RedisAccess with direct host/port and secret-ref credentials.
func CreateRedisAccessWithConnectionSecretRef(
	username,
	namespace,
	generatedSecretName string,
	connection controller.ConnectionDetails,
	secretName string,
	aclRules []string,
) error {
	redisYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: RedisAccess
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
  aclRules:
%s
`, username, namespace, generatedSecretName, username, connection.Host, connection.Port, secretName, secretName, formatRedisACLRulesYAML(aclRules, "    "))

	return ApplyManifest(redisYAML)
}

// CreateRedisAccessFromSecretReference creates a RedisAccess referencing an existing connection secret.
func CreateRedisAccessFromSecretReference(
	username,
	namespace,
	generatedSecretName,
	connSecret string,
	connSecretNamespace *string,
	aclRules []string,
) error {
	existingSecretNamespaceYAML := ""
	if connSecretNamespace != nil && strings.TrimSpace(*connSecretNamespace) != "" {
		existingSecretNamespaceYAML = fmt.Sprintf("    existingSecretNamespace: %s\n", strings.TrimSpace(*connSecretNamespace))
	}

	redisYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: RedisAccess
metadata:
  name: %s
  namespace: %s
spec:
  generatedSecret: %s
  username: %s
  connection:
    existingSecret: %s
%s
  aclRules:
%s
`, username, namespace, generatedSecretName, username, connSecret, existingSecretNamespaceYAML, formatRedisACLRulesYAML(aclRules, "    "))

	return ApplyManifest(redisYAML)
}

// WaitForRedisUserState waits until a Redis ACL user either exists or is removed.
func WaitForRedisUserState(namespace string, connection controller.ConnectionDetails, username string, shouldExist bool) {
	Eventually(func(g Gomega) {
		output, err := RunRedisCLI(namespace, connection, "ACL", "USERS")
		g.Expect(err).NotTo(HaveOccurred(), "Failed to list Redis users")
		g.Expect(redisOutputContainsLine(output, username)).To(Equal(shouldExist))
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// WaitForRedisACLRules waits until the Redis ACL line contains all expected rules for the user.
func WaitForRedisACLRules(namespace string, connection controller.ConnectionDetails, username string, expectedRules []string) {
	Eventually(func(g Gomega) {
		output, err := RunRedisCLI(namespace, connection, "ACL", "LIST")
		g.Expect(err).NotTo(HaveOccurred(), "Failed to list Redis ACL rules")

		line, found := redisACLLineForUser(output, username)
		g.Expect(found).To(BeTrue(), "expected Redis ACL LIST to include user %s", username)
		g.Expect(line).To(ContainSubstring(" on "))
		for _, rule := range expectedRules {
			g.Expect(line).To(ContainSubstring(strings.TrimSpace(rule)))
		}
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// WaitForRedisAuthenticationSuccess waits until Redis accepts the provided ACL credentials.
func WaitForRedisAuthenticationSuccess(namespace, username, password string) {
	Eventually(func(g Gomega) {
		output, err := runRedisAuth(namespace, username, password)
		g.Expect(err).NotTo(HaveOccurred(), "Redis should accept the expected credentials")
		g.Expect(strings.TrimSpace(output)).To(Equal("OK"))
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// DeleteRedisAccess deletes a RedisAccess resource.
func DeleteRedisAccess(name, namespace string) error {
	cmd := exec.Command("kubectl", "delete", "redisaccess", name, "-n", namespace)
	_, err := Run(cmd)
	return err
}

func runRedisAuth(namespace, username, password string) (string, error) {
	podName, err := getRedisPodName(namespace)
	if err != nil {
		return "", err
	}

	cmd := exec.Command(
		"kubectl",
		"exec",
		"-n",
		namespace,
		"pod/"+podName,
		"-c",
		"redis",
		"--",
		"redis-cli",
		"--raw",
		"AUTH",
		username,
		password,
	)
	output, err := Run(cmd)
	return strings.TrimSpace(output), err
}

func getRedisPodName(namespace string) (string, error) {
	cmd := exec.Command(
		"kubectl",
		"get",
		"pods",
		"-n",
		namespace,
		"-l",
		"app=redis",
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
		return "", fmt.Errorf("failed to find Redis pod in namespace %q", namespace)
	}

	cmd = exec.Command("kubectl", "wait", "--for=condition=Ready", "pod/"+podName, "-n", namespace, "--timeout=30s")
	_, err = Run(cmd)
	if err != nil {
		return "", fmt.Errorf("pod %q is not ready for Redis exec: %w", podName, err)
	}

	return podName, nil
}

func formatRedisACLRulesYAML(rules []string, indent string) string {
	lines := make([]string, 0, len(rules))
	for _, rule := range rules {
		lines = append(lines, fmt.Sprintf("%s- %q", indent, rule))
	}
	return strings.Join(lines, "\n")
}

func redisOutputContainsLine(output, expected string) bool {
	for _, line := range strings.Split(output, "\n") {
		if strings.TrimSpace(line) == expected {
			return true
		}
	}
	return false
}

func redisACLLineForUser(output, username string) (string, bool) {
	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "user "+username+" ") {
			return trimmed, true
		}
	}
	return "", false
}
