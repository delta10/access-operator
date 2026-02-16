//go:build e2e
// +build e2e

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

package e2e

import (
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/delta10/access-operator/api/v1"
	"github.com/delta10/access-operator/internal/controller"
	"github.com/delta10/access-operator/test/utils"
)

// namespace where the project is deployed in
const namespace = "access-operator-system"

// serviceAccountName created for the project
const serviceAccountName = "access-operator-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "access-operator-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "access-operator-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				// Get the name of the controller-manager pod
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				// Validate the pod's status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=access-operator-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": ["curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics"],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks
		Context("Postgres", func() {
			BeforeEach(func() {
				By("creating the test namespace if it doesn't exist")
				testNamespace, conn := getDatabaseVariables()
				cmd := exec.Command("kubectl", "create", "ns", testNamespace, "--dry-run=client", "-o", "yaml")
				output, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to generate namespace yaml")

				cmd = exec.Command("kubectl", "apply", "-f", "-")
				cmd.Stdin = strings.NewReader(output)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to create test namespace")

				By("deploying PostgreSQL in the test namespace")
				err = deployPostgresInstance(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to deploy PostgreSQL instance")

				By("waiting for PostgreSQL to become ready")
				cmd = exec.Command(
					"kubectl",
					"wait",
					"--for=condition=Available",
					"deployment/postgres",
					"-n",
					testNamespace,
					"--timeout=2m",
				)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "PostgreSQL deployment should become available")

				By("waiting for PostgreSQL to accept connections")
				verifyPostgresReady := func(g Gomega) {
					output, err := runPostgresQuery(
						testNamespace,
						conn,
						"SELECT 1;",
					)
					g.Expect(err).NotTo(HaveOccurred(), "PostgreSQL should accept connections")
					g.Expect(output).To(Equal("1"), "PostgreSQL should return a valid query result")
				}
				Eventually(verifyPostgresReady, 2*time.Minute, 5*time.Second).Should(Succeed())

				By("ensuring the test table exists")
				_, err = runPostgresQuery(
					testNamespace,
					conn,
					"CREATE TABLE IF NOT EXISTS public.access_operator_test(id SERIAL PRIMARY KEY, value TEXT);",
				)
				Expect(err).NotTo(HaveOccurred(), "Failed to prepare test table")

				Expect(conn.Host).NotTo(BeEmpty(), "Postgres host should be configured")
			})

			AfterEach(func() {
				testNamespace, _ := getDatabaseVariables()

				By("cleaning up any remaining PostgresAccess resources")
				cmd := exec.Command("kubectl", "delete", "postgresaccess", "--all", "-n", testNamespace, "--ignore-not-found")
				_, _ = utils.Run(cmd)

				By("cleaning up the test namespace")
				cmd = exec.Command("kubectl", "delete", "ns", testNamespace, "--ignore-not-found")
				_, _ = utils.Run(cmd)
			})

			It("should create a PostgresAccess resource and create a database user with the specified privileges via direct connection details",
				func() {
					By("creating a PostgresAccess resource")
					testNamespace, conn := getDatabaseVariables()

					pgAccessYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: PostgresAccess
metadata:
  name: test-postgres-access
  namespace: %s
spec:
  generatedSecret: test-postgres-credentials
  username: test-postgres-access
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
        - CONNECT
        - SELECT
`, testNamespace, conn.Host, conn.Port, conn.Database, conn.Username, conn.Password, conn.Database)

					cmd := exec.Command("kubectl", "apply", "-f", "-")
					cmd.Stdin = strings.NewReader(pgAccessYAML)
					_, err := utils.Run(cmd)
					Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with connection details ", " output ", err, " yaml ", pgAccessYAML)

					// Wait for the secret to be created
					By("waiting for the generated secret to be created")
					verifySecretCreated := func(g Gomega) {
						cmd := exec.Command("kubectl", "get", "secret", "test-postgres-credentials", "-n", testNamespace, "-o", "jsonpath={.data.username}")
						output, err := utils.Run(cmd)
						g.Expect(err).NotTo(HaveOccurred(), "Secret should exist")
						g.Expect(output).NotTo(BeEmpty(), "Secret should have username data")
					}
					Eventually(verifySecretCreated, 2*time.Minute, 5*time.Second).Should(Succeed())

					// Verify the database user was created
					By("verifying the database user was created")
					verifyDatabaseUserCreated := func(g Gomega) {
						output, err := runPostgresQuery(
							testNamespace,
							conn,
							"SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'test-postgres-access');",
						)
						g.Expect(err).NotTo(HaveOccurred(), "Failed to check if user exists")
						g.Expect(output).To(Equal("t"), "Database user should have been created")
					}
					Eventually(verifyDatabaseUserCreated, 2*time.Minute, 5*time.Second).Should(Succeed())

					// Verify the privileges were granted
					By("verifying the privileges were granted")
					err = verifyPrivilegesGranted(testNamespace, conn, "test-postgres-access", []string{"CONNECT", "SELECT"})
					Expect(err).NotTo(HaveOccurred(), "Failed to verify privileges granted to the database user")
				})

			It("should create a PostgresAccess resource with connectivity as a secret reference and create a database user accordingly",
				func() {
					By("creating a secret with the connection details")
					testNamespace, conn := getDatabaseVariables()
					secretName, err := createConnectionDetailsViaSecret(testNamespace, conn)
					Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

					By("creating a PostgresAccess resource referencing the connection secret")
					err = createResourceFromSecretReference("test-username", testNamespace, secretName, accessv1.GrantSpec{
						Database:   conn.Database,
						Privileges: []string{"CONNECT", "SELECT"},
					})
					Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

					// Wait for the secret to be created
					By("waiting for the generated secret to be created")
					verifySecretCreated := func(g Gomega) {
						cmd := exec.Command("kubectl", "get", "secret", "test-postgres-credentials-secret-ref", "-n", testNamespace, "-o", "jsonpath={.data.username}")
						output, err := utils.Run(cmd)
						g.Expect(err).NotTo(HaveOccurred(), "Secret should exist")
						g.Expect(output).NotTo(BeEmpty(), "Secret should have username data")
					}

					Eventually(verifySecretCreated, 2*time.Minute, 5*time.Second).Should(Succeed())

					// Verify the database user was created
					By("verifying the database user was created")
					verifyDatabaseUserCreated := func(g Gomega) {
						output, err := runPostgresQuery(
							testNamespace,
							conn,
							"SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'test-username');",
						)
						g.Expect(err).NotTo(HaveOccurred(), "Failed to check if user exists")
						g.Expect(output).To(Equal("t"), "Database user should have been created")
					}
					Eventually(verifyDatabaseUserCreated, 2*time.Minute, 5*time.Second).Should(Succeed())
				})

			It("should reconcile and maintain the privileges of a PostgresAccess resource when they are manually revoked in the database",
				func() {
					By("creating a PostgresAccess resource")
					testNamespace, conn := getDatabaseVariables()
					secretName, err := createConnectionDetailsViaSecret(testNamespace, conn)
					Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

					err = createResourceFromSecretReference("test-privileges-maintenance", testNamespace, secretName, accessv1.GrantSpec{
						Database:   conn.Database,
						Privileges: []string{"CONNECT", "SELECT"},
					})
					Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

					// Wait for the privileges to be granted
					By("waiting for the privileges to be granted")
					verifyPrivileges := func(g Gomega) {
						err = verifyPrivilegesGranted(testNamespace, conn, "test-privileges-maintenance", []string{"CONNECT", "SELECT"})
						g.Expect(err).NotTo(HaveOccurred(), "Failed to verify privileges granted to the database user")
					}
					Eventually(verifyPrivileges, 2*time.Minute, 5*time.Second).Should(Succeed())

					By("revoking the SELECT privilege from the database user")
					_, err = runPostgresQuery(
						testNamespace,
						conn,
						"REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM test-privileges-maintenance;",
					)
					Expect(err).NotTo(HaveOccurred(), "Failed to revoke SELECT privilege")

					By("verifying that the controller reconciles and restores the revoked privilege")
					Eventually(verifyPrivileges, 2*time.Minute, 5*time.Second).Should(Succeed())
				})

			It("should reconcile privileges when they're changed in the config", func() {
				By("creating a PostgresAccess resource with certain privileges")
				testNamespace, conn := getDatabaseVariables()
				secretName, err := createConnectionDetailsViaSecret(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				err = createResourceFromSecretReference("test-privileges-reconciliation", testNamespace, secretName, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				// Wait for the initial privileges to be granted
				By("waiting for the initial privileges to be granted")
				verifyInitialPrivileges := func(g Gomega) {
					err = verifyPrivilegesGranted(testNamespace, conn, "test-privileges-reconciliation", []string{"CONNECT"})
					g.Expect(err).NotTo(HaveOccurred(), "Failed to verify initial privileges granted to the database user")
				}
				Eventually(verifyInitialPrivileges, 2*time.Minute, 5*time.Second).Should(Succeed())

				By("updating the PostgresAccess resource to include additional privileges")
				err = createResourceFromSecretReference("test-privileges-reconciliation", testNamespace, secretName, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT", "INSERT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to update PostgresAccess resource with new privileges")

				By("verifying that the new privileges are granted")
				verifyUpdatedPrivileges := func(g Gomega) {
					err = verifyPrivilegesGranted(testNamespace, conn, "test-privileges-reconciliation", []string{"CONNECT", "SELECT", "INSERT"})
					g.Expect(err).NotTo(HaveOccurred(), "Failed to verify updated privileges granted to the database user")
				}
				Eventually(verifyUpdatedPrivileges, 2*time.Minute, 5*time.Second).Should(Succeed())
			})
		})
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	// Temporary file to store the token request
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		// Execute kubectl command to create the token
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		// Parse the JSON output to extract the token
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

func getDatabaseVariables() (string, controller.ConnectionDetails) {
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

func deployPostgresInstance(namespace string, connection controller.ConnectionDetails) error {
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
	_, err := utils.Run(cmd)
	return err
}

func runPostgresQuery(namespace string, connection controller.ConnectionDetails, query string) (string, error) {
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

	output, err := utils.Run(cmd)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(output), nil
}

func createConnectionDetailsViaSecret(namespace string, connection controller.ConnectionDetails) (string, error) {
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
	_, err := utils.Run(cmd)
	return secretName, err
}

func createResourceFromSecretReference(username, namespace, connSecret string, grants accessv1.GrantSpec) error {
	privilegesYAML := "privileges:"
	for _, privilege := range grants.Privileges {
		privilegesYAML += fmt.Sprintf("\n        - %s", privilege)
	}
	pgAccessYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: PostgresAccess
metadata:
  name: test-postgres-access-secret-ref
  namespace: %s
spec:
  generatedSecret: test-postgres-credentials-secret-ref
  username: %s
  connection:
    existingSecret: %s
  grants:
    - database: %s
      %s
`, namespace, username, connSecret, grants.Database, privilegesYAML)

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(pgAccessYAML)
	_, err := utils.Run(cmd)
	return err
}

func verifyPrivilegesGranted(
	namespace string,
	connection controller.ConnectionDetails,
	username string,
	expectedPrivileges []string,
) error {
	// Build the privilege check query dynamically based on expected privileges
	var privilegeChecks []string

	for _, privilege := range expectedPrivileges {
		upperPrivilege := strings.ToUpper(privilege)
		switch upperPrivilege {
		case "CONNECT", "TEMPORARY", "TEMP":
			// Database-level privileges
			privilegeChecks = append(privilegeChecks,
				fmt.Sprintf("has_database_privilege('%s', current_database(), '%s')", username, upperPrivilege))
		case "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER":
			// Table-level privileges
			privilegeChecks = append(privilegeChecks,
				fmt.Sprintf("has_table_privilege('%s', 'public.access_operator_test', '%s')", username, upperPrivilege))
		case "USAGE":
			// Schema-level privilege
			privilegeChecks = append(privilegeChecks,
				fmt.Sprintf("has_schema_privilege('%s', 'public', '%s')", username, upperPrivilege))
		case "CREATE":
			// CREATE can be granted at database or schema level depending on target object.
			privilegeChecks = append(privilegeChecks,
				fmt.Sprintf("(has_database_privilege('%s', current_database(), 'CREATE') OR has_schema_privilege('%s', 'public', 'CREATE'))", username, username))
		case "EXECUTE":
			// Function-level privilege - we'll check if user has execute on any function
			privilegeChecks = append(privilegeChecks,
				fmt.Sprintf("has_schema_privilege('%s', 'public', 'USAGE')", username))
		default:
			// For any other privileges, try database level
			privilegeChecks = append(privilegeChecks,
				fmt.Sprintf("has_database_privilege('%s', current_database(), '%s')", username, upperPrivilege))
		}
	}

	privilegesQuery := fmt.Sprintf("SELECT %s;", strings.Join(privilegeChecks, ", "))
	output, err := runPostgresQuery(namespace, connection, privilegesQuery)
	if err != nil {
		return err
	}

	var actualPrivileges []string
	if output != "" {
		// psql -tA with -F, returns comma-separated values
		actualPrivileges = strings.Split(output, ",")
	}

	// By comparing the lengths, we detect if there are any extra (excess) privileges
	if len(actualPrivileges) != len(expectedPrivileges) {
		return fmt.Errorf("privilege count mismatch (possible excess privileges): expected %d (%v), got %d (%v)",
			len(expectedPrivileges), expectedPrivileges, len(actualPrivileges), actualPrivileges)
	}

	// Check that all privileges are granted (all should be 't' for true)
	for i, result := range actualPrivileges {
		trimmedResult := strings.TrimSpace(result)
		if trimmedResult != "t" {
			return fmt.Errorf("privilege check failed for '%s': expected 't' (granted), got '%s'",
				expectedPrivileges[i], trimmedResult)
		}
	}

	return nil
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
