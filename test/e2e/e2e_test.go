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
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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
				testNamespace, _, _, _, _, _ := getDatabaseVariables()
				cmd := exec.Command("kubectl", "create", "ns", testNamespace, "--dry-run=client", "-o", "yaml")
				output, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to generate namespace yaml")

				cmd = exec.Command("kubectl", "apply", "-f", "-")
				cmd.Stdin = strings.NewReader(output)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to create test namespace")
			})

			AfterEach(func() {
				By("cleaning up any remaining PostgresAccess resources")
				cmd := exec.Command("kubectl", "delete", "postgresaccess", "--all", "-n", "postgres-access-test", "--ignore-not-found")
				_, _ = utils.Run(cmd)

				By("cleaning up the test namespace")
				cmd = exec.Command("kubectl", "delete", "ns", "postgres-access-test", "--ignore-not-found")
				_, _ = utils.Run(cmd)
			})

			It("should create a PostgresAccess resource and verify database connectivity",
				func() {
					By("creating a PostgresAccess resource")
					testNamespace, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDB := getDatabaseVariables()
					conn, err := connectToDB()

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
    username:
      value: %s
    password:
      value: %s
  grants:
    - database: %s
      privileges:
        - CONNECT
        - SELECT
`, testNamespace, postgresHost, postgresPort, postgresDB, postgresUser, postgresPassword, postgresDB)

					cmd := exec.Command("kubectl", "apply", "-f", "-")
					cmd.Stdin = strings.NewReader(pgAccessYAML)
					_, err = utils.Run(cmd)
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
					var userExists bool
					err = conn.QueryRow(context.Background(),
						"SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)",
						"test-postgres-access").Scan(&userExists)
					Expect(err).NotTo(HaveOccurred(), "Failed to check if user exists")
					Expect(userExists).To(BeTrue(), "Database user should have been created")

					// Verify the privileges were granted
					By("verifying the privileges were granted")
					var hasConnectPrivilege, hasSelectPrivilege bool
					err = conn.QueryRow(context.Background(), `
                SELECT 
                    has_database_privilege('test-postgres-access', 'testdb', 'CONNECT') AS has_connect,
                    has_database_privilege('test-postgres-access', 'testdb', 'SELECT') AS has_select
            `).Scan(&hasConnectPrivilege, &hasSelectPrivilege)
					Expect(err).NotTo(HaveOccurred(), "Failed to check user privileges")
					Expect(hasConnectPrivilege).To(BeTrue(), "User should have CONNECT privilege")
					Expect(hasSelectPrivilege).To(BeTrue(), "User should have SELECT privilege")

					// Cleanup
					By("cleaning up the PostgresAccess resource")
					cmd = exec.Command("kubectl", "delete", "postgresaccess", "test-postgres-access", "-n", testNamespace, "--ignore-not-found")
					_, _ = utils.Run(cmd)

					cmd = exec.Command("kubectl", "delete", "ns", testNamespace, "--ignore-not-found")
					_, _ = utils.Run(cmd)
				})

			It("should create a PostgresAccess resource with with connectivity as a secret reference and verify database connectivity",
				func() {
					By("creating a secret with the connection details")
					testNamespace, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDB := getDatabaseVariables()
					conn, err := connectToDB()
					connectionSecretYAML :=
						fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: postgres-connection-secret
  namespace: %s
type: Opaque
data:
  host: %s
  port: %s
  database: %s
  username: %s
  password: %s
`, testNamespace,
							b64.StdEncoding.EncodeToString([]byte(postgresHost)),
							b64.StdEncoding.EncodeToString([]byte(postgresPort)),
							b64.StdEncoding.EncodeToString([]byte(postgresDB)),
							b64.StdEncoding.EncodeToString([]byte(postgresUser)),
							b64.StdEncoding.EncodeToString([]byte(postgresPassword)))

					cmd := exec.Command("kubectl", "apply", "-f", "-")
					cmd.Stdin = strings.NewReader(connectionSecretYAML)
					_, err = utils.Run(cmd)
					Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

					By("creating a PostgresAccess resource referencing the connection secret")
					pgAccessYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: PostgresAccess
metadata:
  name: test-postgres-access-secret-ref
  namespace: %s
spec:
  generatedSecret: test-postgres-credentials-secret-ref
  username: test-postgres-access-secret-ref
  connection:
    existingSecret: postgres-connection-secret
  grants:
    - database: %s
      privileges:
        - CONNECT
        - SELECT
`, testNamespace, postgresDB)

					cmd = exec.Command("kubectl", "apply", "-f", "-")
					cmd.Stdin = strings.NewReader(pgAccessYAML)
					_, err = utils.Run(cmd)
					Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

					// Wait for the secret to be created
					By("waiting for the generated secret to be created")
					verifySecretCreated := func(g Gomega) {
						_ = exec.Command("kubectl", "get", "secret", "test-postgres-credentials-secret-ref", "-n", testNamespace, "-o", "jsonpath={.data.username}")
					}

					Eventually(verifySecretCreated, 2*time.Minute, 5*time.Second).Should(Succeed())

					// Verify the database user was created
					By("verifying the database user was created")
					var userExists bool
					err = conn.QueryRow(context.Background(),
						"SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)",
						"test-postgres-access-secret-ref").Scan(&userExists)
					Expect(err).NotTo(HaveOccurred(), "Failed to check if user exists")
					Expect(userExists).To(BeTrue(), "Database user should have been created")
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

func getDatabaseVariables() (string, string, string, string, string, string) {
	testNamespace := "postgres-access-test"

	postgresHost := os.Getenv("POSTGRES_HOST")
	if postgresHost == "" {
		postgresHost = "postgres" // Use service name when running in CI
	}
	postgresPort := os.Getenv("POSTGRES_PORT")
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

	return testNamespace, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDB

}

func connectToDB() (*pgx.Conn, error) {
	_, postgresHost, postgresPort, postgresUser, postgresPassword, postgresDB := getDatabaseVariables()
	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=disable",
		postgresUser, postgresPassword, postgresHost, postgresPort, postgresDB)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return pgx.Connect(ctx, connStr)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
