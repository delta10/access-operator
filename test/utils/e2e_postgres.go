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
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/gomega" // nolint:revive,staticcheck

	accessv1 "github.com/delta10/access-operator/api/v1"
	"github.com/delta10/access-operator/internal/controller"
)

const (
	cnpgOperatorManifestURL   = "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.28/releases/cnpg-1.28.1.yaml"
	cnpgSystemNamespace       = "cnpg-system"
	cnpgWebhookServiceName    = "cnpg-webhook-service"
	cnpgControllerDeployment  = "cnpg-controller-manager"
	cnpgClusterName           = "cnpg-postgres"
	cnpgWebhookStartupTimeout = 3 * time.Minute
)

// DatabaseConnectionDetailsForNamespace returns connection details for the provided postgres namespace.
func DatabaseConnectionDetailsForNamespace(testNamespace string) controller.ConnectionDetails {
	defaultClusterHost := fmt.Sprintf("postgres.%s.svc", testNamespace)

	postgresHost := os.Getenv("POSTGRES_CLUSTER_HOST")
	if postgresHost == "" {
		postgresHost = os.Getenv("POSTGRES_HOST")
	}
	if postgresHost == "" || postgresHost == "localhost" || postgresHost == "127.0.0.1" || postgresHost == "postgres" {
		postgresHost = defaultClusterHost
	}

	postgresPort := os.Getenv("POSTGRES_CLUSTER_PORT")
	if postgresPort == "" {
		postgresPort = os.Getenv("POSTGRES_PORT")
	}
	if postgresPort == "" {
		postgresPort = "5432"
	}

	postgresUser := os.Getenv("POSTGRES_USER")
	if postgresUser == "" {
		postgresUser = "postgres"
	}

	postgresPassword := os.Getenv("POSTGRES_PASSWORD")
	if postgresPassword == "" {
		postgresPassword = "postgres"
	}

	postgresDB := os.Getenv("POSTGRES_DB")
	if postgresDB == "" {
		postgresDB = "testdb"
	}

	return controller.ConnectionDetails{
		SharedConnectionDetails: controller.SharedConnectionDetails{
			Host:     postgresHost,
			Port:     postgresPort,
			Username: postgresUser,
			Password: postgresPassword,
		},
		Database: postgresDB,
		SSLMode:  "disable",
	}
}

// GetDatabaseVariables returns the namespace and connection details for e2e postgres tests.
func GetDatabaseVariables() (string, controller.ConnectionDetails) {
	testNamespace := os.Getenv("POSTGRES_TEST_NAMESPACE")
	if testNamespace == "" {
		testNamespace = "postgres-access-test"
	}

	return testNamespace, DatabaseConnectionDetailsForNamespace(testNamespace)
}

func GetCNPGConnectionDetailsFromSecret(namespace, secretName string) controller.ConnectionDetails {
	var conn controller.ConnectionDetails
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "secret", secretName, "-n", namespace, "-o", "jsonpath={.data}")
		output, err := Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to get CNPG connection secret")

		var data map[string]string
		err = json.Unmarshal([]byte(output), &data)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to parse CNPG connection secret data")

		host, err := b64.StdEncoding.DecodeString(data["host"])
		g.Expect(err).NotTo(HaveOccurred(), "Failed to decode host")
		port, err := b64.StdEncoding.DecodeString(data["port"])
		g.Expect(err).NotTo(HaveOccurred(), "Failed to decode port")
		dbname, err := b64.StdEncoding.DecodeString(data["dbname"])
		g.Expect(err).NotTo(HaveOccurred(), "Failed to decode dbname")
		username, err := b64.StdEncoding.DecodeString(data["username"])
		g.Expect(err).NotTo(HaveOccurred(), "Failed to decode username")
		password, err := b64.StdEncoding.DecodeString(data["password"])
		g.Expect(err).NotTo(HaveOccurred(), "Failed to decode password")

		conn.Host = string(host)
		conn.Port = string(port)
		conn.Database = string(dbname)
		conn.Username = string(username)
		conn.Password = string(password)
	}, 2*time.Minute, 5*time.Second).Should(Succeed())

	return conn
}

// DeployPostgresInstance deploys a postgres service/deployment into the provided namespace.
func DeployPostgresInstance(namespace string, connection controller.ConnectionDetails) error {
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: %s
spec:
  selector:
    app: postgres
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
        - name: postgres
          image: postgres:18.3-alpine
          imagePullPolicy: IfNotPresent
          env:
            - name: POSTGRES_USER
              value: %q
            - name: POSTGRES_PASSWORD
              value: %q
            - name: POSTGRES_DB
              value: %q
          ports:
            - containerPort: 5432
`, namespace, namespace, connection.Username, connection.Password, connection.Database)

	return ApplyManifest(manifest)
}

func DeletePostgresInstance(namespace string) error {
	cmd := exec.Command("kubectl", "delete", "deployment,service", "postgres", "-n", namespace, "--ignore-not-found")
	_, err := Run(cmd)
	return err
}

func DeployCNPGInstance(namespace string) error {
	cmd := exec.Command("kubectl", "apply", "--server-side", "-f",
		cnpgOperatorManifestURL)
	_, err := Run(cmd)
	if err != nil {
		return fmt.Errorf("failed to deploy CNPG operator: %w", err)
	}

	cmd = exec.Command("kubectl", "wait", "--for=condition=Established", "crd/clusters.postgresql.cnpg.io", "--timeout=2m")
	_, err = Run(cmd)
	if err != nil {
		return fmt.Errorf("failed waiting for CNPG Cluster CRD: %w", err)
	}

	cmd = exec.Command("kubectl", "wait", "deployment/"+cnpgControllerDeployment, "-n", cnpgSystemNamespace, "--for=condition=Available", "--timeout=3m")
	_, err = Run(cmd)
	if err != nil {
		return fmt.Errorf("failed waiting for CNPG operator deployment: %w", err)
	}

	cmd = exec.Command("kubectl", "rollout", "status", "deployment/"+cnpgControllerDeployment, "-n", cnpgSystemNamespace, "--timeout=3m")
	_, err = Run(cmd)
	if err != nil {
		return fmt.Errorf("failed waiting for CNPG operator rollout: %w", err)
	}

	clusterManifest := fmt.Sprintf(`apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: 1
  imageName: ghcr.io/cloudnative-pg/postgresql:18.3
  imagePullPolicy: IfNotPresent
  storage:
    size: 1Gi
  bootstrap:
    initdb:
      database: app
      owner: app
      postInitSQL:
        - ALTER ROLE app SUPERUSER;
  postgresql:
    parameters:
      shared_buffers: "256MB"
      max_connections: "100"
`, cnpgClusterName, namespace)

	if err := waitForCNPGWebhookReady(clusterManifest); err != nil {
		return fmt.Errorf("failed waiting for CNPG webhook readiness: %w", err)
	}

	if err := applyCNPGClusterManifest(clusterManifest); err != nil {
		return fmt.Errorf("failed to apply CNPG cluster: %w", err)
	}

	cmd = exec.Command("kubectl", "wait", "cluster/"+cnpgClusterName, "-n", namespace, "--for=condition=Ready", "--timeout=5m")
	_, err = Run(cmd)
	if err != nil {
		return fmt.Errorf("failed waiting for CNPG cluster to become Ready: %w", err)
	}

	cmd = exec.Command(
		"kubectl",
		"wait",
		"--for=condition=Ready",
		"pod",
		"-l",
		"cnpg.io/cluster=cnpg-postgres",
		"-n",
		namespace,
		"--timeout=5m",
	)
	_, err = Run(cmd)
	if err != nil {
		return fmt.Errorf("failed waiting for CNPG pod readiness: %w", err)
	}

	return nil
}

func waitForCNPGWebhookReady(clusterManifest string) error {
	if err := waitForCNPGWebhookEndpoints(); err != nil {
		return err
	}

	var lastErr error
	deadline := time.Now().Add(cnpgWebhookStartupTimeout)
	for time.Now().Before(deadline) {
		lastErr = ApplyManifestServerDryRun(clusterManifest)
		if lastErr == nil {
			return nil
		}

		time.Sleep(5 * time.Second)
	}

	return fmt.Errorf("CNPG admission webhook never became ready: %w", lastErr)
}

func waitForCNPGWebhookEndpoints() error {
	var lastErr error
	deadline := time.Now().Add(cnpgWebhookStartupTimeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command(
			"kubectl",
			"get",
			"endpoints",
			cnpgWebhookServiceName,
			"-n",
			cnpgSystemNamespace,
			"-o",
			"jsonpath={.subsets[*].addresses[*].ip}",
		)

		output, err := Run(cmd)
		lastErr = err
		if err == nil && strings.TrimSpace(output) != "" {
			return nil
		}

		time.Sleep(5 * time.Second)
	}

	if lastErr != nil {
		return fmt.Errorf("timed out waiting for webhook endpoints: %w", lastErr)
	}

	return fmt.Errorf("timed out waiting for webhook endpoints to become ready")
}

func applyCNPGClusterManifest(manifest string) error {
	var lastErr error
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		lastErr = ApplyManifest(manifest)
		if lastErr == nil {
			return nil
		}

		if !isTransientCNPGWebhookError(lastErr) {
			return lastErr
		}

		time.Sleep(5 * time.Second)
	}

	return lastErr
}

func isTransientCNPGWebhookError(err error) bool {
	if err == nil {
		return false
	}

	message := err.Error()
	transientMessages := []string{
		"failed calling webhook",
		"connect: connection refused",
		"no endpoints available for service",
		"service unavailable",
		"context deadline exceeded",
	}

	for _, transientMessage := range transientMessages {
		if strings.Contains(message, transientMessage) {
			return true
		}
	}

	return false
}

func getPostgresExecTarget(namespace string) (string, error) {
	cmd := exec.Command("kubectl", "get", "deployment", "postgres", "-n", namespace, "-o", "name", "--ignore-not-found")
	output, err := Run(cmd)
	if err != nil {
		return "", err
	}

	target := strings.TrimSpace(output)
	if target != "" {
		return "deployment/postgres", nil
	}

	cmd = exec.Command(
		"kubectl",
		"get",
		"pods",
		"-n",
		namespace,
		"-l",
		"cnpg.io/cluster=cnpg-postgres",
		"--field-selector=status.phase=Running",
		"-o",
		"jsonpath={.items[0].metadata.name}",
	)
	output, err = Run(cmd)
	if err != nil {
		return "", err
	}

	podName := strings.TrimSpace(output)
	if podName == "" {
		return "", fmt.Errorf("failed to find SQL exec target in namespace %q", namespace)
	}

	cmd = exec.Command("kubectl", "wait", "--for=condition=Ready", "pod/"+podName, "-n", namespace, "--timeout=30s")
	_, err = Run(cmd)
	if err != nil {
		return "", fmt.Errorf("pod %q is not ready for SQL exec: %w", podName, err)
	}

	return "pod/" + podName, nil
}

// RunPostgresQuery executes a SQL query against the in-cluster PostgreSQL target.
func RunPostgresQuery(namespace string, connection controller.ConnectionDetails, query string) (string, error) {
	target, err := getPostgresExecTarget(namespace)
	if err != nil {
		return "", err
	}

	cmd := exec.Command(
		"kubectl",
		"exec",
		"-n",
		namespace,
		target,
		"-c",
		"postgres",
		"--",
		"env",
		fmt.Sprintf("PGPASSWORD=%s", connection.Password),
		"psql",
		"-h",
		"127.0.0.1",
		"-U",
		connection.Username,
		"-d",
		connection.Database,
		"-tA",
		"-F,",
		"-c",
		query,
	)

	output, err := Run(cmd)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(output), nil
}

// CreateConnectionDetailsViaSecret creates a connection secret used by PostgresAccess.
func CreateConnectionDetailsViaSecret(namespace string, connection controller.ConnectionDetails) (string, error) {
	secretName := "postgres-connection-secret"
	secretYAML := fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
type: Opaque
data:
  host: %s
  port: %s
  dbname: %s
  username: %s
  password: %s
  sslmode: %s
`, secretName, namespace,
		b64.StdEncoding.EncodeToString([]byte(connection.Host)),
		b64.StdEncoding.EncodeToString([]byte(connection.Port)),
		b64.StdEncoding.EncodeToString([]byte(connection.Database)),
		b64.StdEncoding.EncodeToString([]byte(connection.Username)),
		b64.StdEncoding.EncodeToString([]byte(connection.Password)),
		b64.StdEncoding.EncodeToString([]byte("disable")))

	return secretName, ApplyManifest(secretYAML)
}

// CreatePostgresAccessWithDirectConnection creates a PostgresAccess with inline connection fields.
func CreatePostgresAccessWithDirectConnection(
	username,
	namespace,
	generatedSecretName string,
	connection controller.ConnectionDetails,
	privileges []string,
) error {
	pgAccessYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: PostgresAccess
metadata:
  name: %s
  namespace: %s
spec:
  generatedSecret: %s
  username: %s
  connection:
    host: %s
    port: %s
    database: %s
    sslMode: disable
    username:
      value: %s
    password:
      value: %s
  grants:
    - database: %s
      privileges:
%s
`, username, namespace, generatedSecretName, username, connection.Host, connection.Port, connection.Database, connection.Username, connection.Password, connection.Database, formatStringListYAML(privileges, "        "))

	return ApplyManifest(pgAccessYAML)
}

// CreatePostgresAccessWithConnectionSecretRef creates a PostgresAccess that references an existing connection secret for the user and password; assumes they're called 'username' and 'password' here.
func CreatePostgresAccessWithConnectionSecretRef(
	username,
	namespace,
	generatedSecretName string,
	connection controller.ConnectionDetails,
	secretKeyRef string,
	privileges []string,
) error {
	pgAccessYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: PostgresAccess
metadata:
  name: %s
  namespace: %s
spec:
  generatedSecret: %s
  username: %s
  connection:
    host: %s
    port: %s
    database: %s
    sslMode: disable
    username:
      secretRef: 
        name: %s
        key: username
    password:
      secretRef:
        name: %s
        key: password
  grants:
    - database: %s
      privileges:
%s
`, username, namespace, generatedSecretName, username, connection.Host, connection.Port, connection.Database, secretKeyRef, secretKeyRef, connection.Database, formatStringListYAML(privileges, "        "))

	return ApplyManifest(pgAccessYAML)
}

// CreateResourceFromSecretReference creates a PostgresAccess that references an existing secret.
func CreateResourceFromSecretReference(
	username,
	namespace,
	generatedSecretName,
	connSecret string,
	cleanupPolicy *accessv1.CleanupPolicy,
	grants accessv1.GrantSpec,
) error {
	return createResourceFromSecretReference(
		username,
		namespace,
		generatedSecretName,
		connSecret,
		nil,
		cleanupPolicy,
		grants,
	)
}

// CreateResourceFromSecretReferenceWithNamespace creates a PostgresAccess that references an existing secret in another namespace.
func CreateResourceFromSecretReferenceWithNamespace(
	username,
	namespace,
	generatedSecretName,
	connSecret,
	connSecretNamespace string,
	cleanupPolicy *accessv1.CleanupPolicy,
	grants accessv1.GrantSpec,
) error {
	return createResourceFromSecretReference(
		username,
		namespace,
		generatedSecretName,
		connSecret,
		&connSecretNamespace,
		cleanupPolicy,
		grants,
	)
}

func createResourceFromSecretReference(
	username,
	namespace,
	generatedSecretName,
	connSecret string,
	connSecretNamespace *string,
	cleanupPolicy *accessv1.CleanupPolicy,
	grants accessv1.GrantSpec,
) error {
	cleanupPolicyYAML := ""
	if cleanupPolicy != nil && *cleanupPolicy != "" {
		cleanupPolicyYAML = fmt.Sprintf("  cleanupPolicy: %s\n", *cleanupPolicy)
	}

	existingSecretNamespaceYAML := ""
	if connSecretNamespace != nil && strings.TrimSpace(*connSecretNamespace) != "" {
		existingSecretNamespaceYAML = fmt.Sprintf("    existingSecretNamespace: %s\n", strings.TrimSpace(*connSecretNamespace))
	}

	pgAccessYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: PostgresAccess
metadata:
  name: %s
  namespace: %s
spec:
  generatedSecret: %s
  username: %s
  connection:
    existingSecret: %s
%s
%s
  grants:
    - database: %s
      privileges:
%s
`, username, namespace, generatedSecretName, username, connSecret, existingSecretNamespaceYAML, cleanupPolicyYAML, grants.Database, formatStringListYAML(grants.Privileges, "        "))

	return ApplyManifest(pgAccessYAML)
}

// WaitForDatabaseUserState waits until a postgres role either exists or is removed.
func WaitForDatabaseUserState(
	namespace string,
	connection controller.ConnectionDetails,
	username string,
	shouldExist bool,
) {
	expected := "f"
	if shouldExist {
		expected = "t"
	}

	Eventually(func(g Gomega) {
		output, err := RunPostgresQuery(
			namespace,
			connection,
			fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = '%s');", username),
		)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to check if user exists")
		g.Expect(output).To(Equal(expected))

		allUsersOutput, err := RunPostgresQuery(
			namespace,
			connection,
			"SELECT rolname FROM pg_roles;",
		)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to list all users")
		fmt.Printf("Current database users: %s\n", allUsersOutput)
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// WaitForPrivilegesGranted waits until all expected privileges are granted.
func WaitForPrivilegesGranted(
	namespace string,
	connection controller.ConnectionDetails,
	username string,
	expectedPrivileges []string,
) {
	Eventually(func(g Gomega) {
		err := verifyPrivilegesGranted(namespace, connection, username, expectedPrivileges)
		g.Expect(err).NotTo(HaveOccurred())
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// WaitForTableOwner waits until the specified table owner matches expectedOwner.
func WaitForTableOwner(namespace string, connection controller.ConnectionDetails, table, expectedOwner string) {
	Eventually(func(g Gomega) {
		output, err := RunPostgresQuery(
			namespace,
			connection,
			fmt.Sprintf("SELECT tableowner FROM pg_tables WHERE schemaname = 'public' AND tablename = '%s';", table),
		)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to query table owner")
		g.Expect(output).To(Equal(expectedOwner))
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// WaitForAuthenticationSuccess waits until auth succeeds for the provided credentials.
func WaitForAuthenticationSuccess(
	namespace string,
	connection controller.ConnectionDetails,
	username,
	password string,
) {
	updatedConn := connection
	updatedConn.Username = username
	updatedConn.Password = password

	Eventually(func(g Gomega) {
		output, err := RunPostgresQuery(
			namespace,
			updatedConn,
			"SELECT 1;",
		)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to authenticate with the expected credentials")
		g.Expect(output).To(Equal("1"), "PostgreSQL should return a valid query result")
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// DeletePostgresAccess deletes a PostgresAccess resource.
func DeletePostgresAccess(name, namespace string) error {
	cmd := exec.Command("kubectl", "delete", "postgresaccess", name, "-n", namespace)
	_, err := Run(cmd)
	return err
}

func verifyPrivilegesGranted(
	namespace string,
	connection controller.ConnectionDetails,
	username string,
	expectedPrivileges []string,
) error {
	var privilegeChecks []string

	for _, privilege := range expectedPrivileges {
		upperPrivilege := strings.ToUpper(privilege)
		switch upperPrivilege {
		case "CONNECT", "TEMPORARY", "TEMP":
			privilegeChecks = append(privilegeChecks,
				fmt.Sprintf("has_database_privilege('%s', current_database(), '%s')", username, upperPrivilege))
		case "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER":
			privilegeChecks = append(privilegeChecks,
				fmt.Sprintf("has_table_privilege('%s', 'public.access_operator_test', '%s')", username, upperPrivilege))
		case "USAGE":
			privilegeChecks = append(privilegeChecks,
				fmt.Sprintf("has_schema_privilege('%s', 'public', '%s')", username, upperPrivilege))
		case "CREATE":
			privilegeChecks = append(privilegeChecks,
				fmt.Sprintf("(has_database_privilege('%s', current_database(), 'CREATE') OR has_schema_privilege('%s', 'public', 'CREATE'))", username, username))
		case "EXECUTE":
			privilegeChecks = append(privilegeChecks,
				fmt.Sprintf("has_schema_privilege('%s', 'public', 'USAGE')", username))
		default:
			privilegeChecks = append(privilegeChecks,
				fmt.Sprintf("has_database_privilege('%s', current_database(), '%s')", username, upperPrivilege))
		}
	}

	privilegesQuery := fmt.Sprintf("SELECT %s;", strings.Join(privilegeChecks, ", "))
	output, err := RunPostgresQuery(namespace, connection, privilegesQuery)
	if err != nil {
		return err
	}

	var actualPrivileges []string
	if output != "" {
		actualPrivileges = strings.Split(output, ",")
	}

	if len(actualPrivileges) != len(expectedPrivileges) {
		return fmt.Errorf("privilege count mismatch (possible excess privileges): expected %d (%v), got %d (%v)",
			len(expectedPrivileges), expectedPrivileges, len(actualPrivileges), actualPrivileges)
	}

	for i, result := range actualPrivileges {
		trimmedResult := strings.TrimSpace(result)
		if trimmedResult != "t" {
			return fmt.Errorf("privilege check failed for '%s': expected 't' (granted), got '%s'",
				expectedPrivileges[i], trimmedResult)
		}
	}

	return nil
}
