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
    "path/filepath"
    "strings"
    "time"

    . "github.com/onsi/ginkgo/v2" // nolint:revive,staticcheck
    . "github.com/onsi/gomega"    // nolint:revive,staticcheck

    accessv1 "github.com/delta10/access-operator/api/v1"
    "github.com/delta10/access-operator/internal/controller"
)

// ServiceAccountToken returns a token for the given service account in a namespace.
func ServiceAccountToken(namespace, serviceAccountName string) (string, error) {
    const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

    secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
    tokenRequestFile := filepath.Join(os.TempDir(), secretName)
    if err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), 0o644); err != nil {
        return "", err
    }

    var out string
    verifyTokenCreation := func(g Gomega) {
        cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
            "/api/v1/namespaces/%s/serviceaccounts/%s/token",
            namespace,
            serviceAccountName,
        ), "-f", tokenRequestFile)

        output, err := Run(cmd)
        g.Expect(err).NotTo(HaveOccurred())

        var token tokenRequest
        err = json.Unmarshal([]byte(output), &token)
        g.Expect(err).NotTo(HaveOccurred())

        out = token.Status.Token
    }
    Eventually(verifyTokenCreation).Should(Succeed())

    return out, nil
}

// GetMetricsOutput returns logs from the metrics curl pod.
func GetMetricsOutput(namespace, podName string) (string, error) {
    By("getting the curl-metrics logs")
    cmd := exec.Command("kubectl", "logs", podName, "-n", namespace)
    return Run(cmd)
}

// GetDatabaseVariables returns the namespace and connection details for e2e postgres tests.
func GetDatabaseVariables() (string, controller.ConnectionDetails) {
    testNamespace := "postgres-access-test"
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

    return testNamespace, controller.ConnectionDetails{
        Host:     postgresHost,
        Port:     postgresPort,
        Database: postgresDB,
        Username: postgresUser,
        Password: postgresPassword,
    }
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
          image: postgres:15-alpine
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

    cmd := exec.Command("kubectl", "apply", "-f", "-")
    cmd.Stdin = strings.NewReader(manifest)
    _, err := Run(cmd)
    return err
}

// RunPostgresQuery executes a SQL query against the in-cluster postgres deployment.
func RunPostgresQuery(namespace string, connection controller.ConnectionDetails, query string) (string, error) {
    cmd := exec.Command(
        "kubectl",
        "exec",
        "-n",
        namespace,
        "deployment/postgres",
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
  database: %s
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

    cmd := exec.Command("kubectl", "apply", "-f", "-")
    cmd.Stdin = strings.NewReader(secretYAML)
    _, err := Run(cmd)
    return secretName, err
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
`, username, namespace, generatedSecretName, username, connection.Host, connection.Port, connection.Database, connection.Username, connection.Password, connection.Database, formatPrivilegesYAML(privileges, "        "))

    cmd := exec.Command("kubectl", "apply", "-f", "-")
    cmd.Stdin = strings.NewReader(pgAccessYAML)
    _, err := Run(cmd)
    return err
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
    cleanupPolicyYAML := ""
    if cleanupPolicy != nil && *cleanupPolicy != "" {
        cleanupPolicyYAML = fmt.Sprintf("  cleanupPolicy: %s\n", *cleanupPolicy)
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
  grants:
    - database: %s
      privileges:
%s
`, username, namespace, generatedSecretName, username, connSecret, cleanupPolicyYAML, grants.Database, formatPrivilegesYAML(grants.Privileges, "        "))

    cmd := exec.Command("kubectl", "apply", "-f", "-")
    cmd.Stdin = strings.NewReader(pgAccessYAML)
    _, err := Run(cmd)
    return err
}

// WaitForSecretField waits until a secret data field is present and returns its value.
func WaitForSecretField(namespace, secretName, field string) string {
    var output string
    Eventually(func(g Gomega) {
        cmd := exec.Command("kubectl", "get", "secret", secretName, "-n", namespace, "-o", fmt.Sprintf("jsonpath={.data.%s}", field))
        var err error
        output, err = Run(cmd)
        g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve secret field")
        g.Expect(strings.TrimSpace(output)).NotTo(BeEmpty(), "Secret field should not be empty")
    }, 2*time.Minute, 5*time.Second).Should(Succeed())

    return strings.TrimSpace(output)
}

// WaitForDecodedSecretField waits for a secret field and returns decoded base64 data.
func WaitForDecodedSecretField(namespace, secretName, field string) string {
    encoded := WaitForSecretField(namespace, secretName, field)
    decoded, err := b64.StdEncoding.DecodeString(encoded)
    Expect(err).NotTo(HaveOccurred(), "Secret value should be valid base64")
    return string(decoded)
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

// WaitForSecretDeleted waits until a secret is no longer found.
func WaitForSecretDeleted(namespace, secretName string) {
    Eventually(func(g Gomega) {
        cmd := exec.Command("kubectl", "get", "secret", secretName, "-n", namespace, "-o", "name", "--ignore-not-found")
        output, err := Run(cmd)
        g.Expect(err).NotTo(HaveOccurred(), "Failed to check if secret exists")
        g.Expect(strings.TrimSpace(output)).To(BeEmpty(), "Secret should have been deleted")
    }, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// TriggerReconciliation forces a reconcile by updating an annotation.
func TriggerReconciliation(resourceName, namespace string) error {
    timestamp := time.Now().Format(time.RFC3339Nano)
    cmd := exec.Command("kubectl", "annotate", "postgresaccess", resourceName,
        "-n", namespace,
        fmt.Sprintf("reconcile-trigger=%s", timestamp),
        "--overwrite")
    _, err := Run(cmd)
    return err
}

func formatPrivilegesYAML(privileges []string, indent string) string {
    lines := make([]string, 0, len(privileges))
    for _, privilege := range privileges {
        lines = append(lines, fmt.Sprintf("%s- %s", indent, privilege))
    }
    return strings.Join(lines, "\n")
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

type tokenRequest struct {
    Status struct {
        Token string `json:"token"`
    } `json:"status"`
}
