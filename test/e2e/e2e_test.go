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
				By("ensuring no stale test namespace exists")
				testNamespace, conn := getDatabaseVariables()
				cmd := exec.Command("kubectl", "delete", "ns", testNamespace, "--ignore-not-found", "--timeout=1m")
				_, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to clean up existing test namespace")

				By("creating a fresh test namespace")
				cmd = exec.Command("kubectl", "create", "ns", testNamespace)
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
				cmd := exec.Command("kubectl", "delete", "postgresaccess", "--all", "-n", testNamespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cmd)

				By("cleaning up the test namespace")
				cmd = exec.Command("kubectl", "delete", "ns", testNamespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cmd)
			})

			It("should create a PostgresAccess resource and create a database user with the specified privileges via direct connection details", func() {
				testNamespace, conn := getDatabaseVariables()
				resourceName := "test-postgres-access"
				generatedSecret := "test-postgres-credentials"

				By("creating a PostgresAccess resource")
				err := createPostgresAccessWithDirectConnection(
					resourceName,
					testNamespace,
					generatedSecret,
					conn,
					[]string{"CONNECT", "SELECT"},
				)
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with connection details")

				By("waiting for the generated secret to be created")
				waitForSecretField(testNamespace, generatedSecret, "username")

				By("verifying the database user was created")
				waitForDatabaseUserState(testNamespace, conn, resourceName, true)

				By("verifying the privileges were granted")
				waitForPrivilegesGranted(testNamespace, conn, resourceName, []string{"CONNECT", "SELECT"})
			})

			It("should create a PostgresAccess resource with connectivity as a secret reference and create a database user accordingly", func() {
				By("creating a secret with the connection details")
				testNamespace, conn := getDatabaseVariables()
				secretName, err := createConnectionDetailsViaSecret(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				By("creating a PostgresAccess resource referencing the connection secret")
				err = createResourceFromSecretReference("test-username", testNamespace, "test-postgres-credentials-secret-ref", secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the generated secret to be created")
				waitForSecretField(testNamespace, "test-postgres-credentials-secret-ref", "username")

				By("verifying the database user was created")
				waitForDatabaseUserState(testNamespace, conn, "test-username", true)
			})

			It("should reconcile privileges when they're changed in the config", func() {
				By("creating a PostgresAccess resource with certain privileges")
				testNamespace, conn := getDatabaseVariables()
				secretName, err := createConnectionDetailsViaSecret(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				err = createResourceFromSecretReference("test-privileges-reconciliation", testNamespace, "test-postgres-credentials-secret-ref", secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the initial privileges to be granted")
				waitForPrivilegesGranted(testNamespace, conn, "test-privileges-reconciliation", []string{"CONNECT"})

				By("updating the PostgresAccess resource to include additional privileges")
				err = createResourceFromSecretReference("test-privileges-reconciliation", testNamespace, "test-postgres-credentials-secret-ref", secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT", "INSERT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to update PostgresAccess resource with new privileges")

				By("verifying that the new privileges are granted")
				waitForPrivilegesGranted(testNamespace, conn, "test-privileges-reconciliation", []string{"CONNECT", "SELECT", "INSERT"})
			})

			It("should reconcile the privileges of a PostgresAccess resource when they are manually revoked in the database", func() {
				By("creating a PostgresAccess resource")
				testNamespace, conn := getDatabaseVariables()
				secretName, err := createConnectionDetailsViaSecret(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				err = createResourceFromSecretReference("test-privileges-maintenance", testNamespace, "test-postgres-credentials-secret-ref", secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the privileges to be granted")
				waitForPrivilegesGranted(testNamespace, conn, "test-privileges-maintenance", []string{"CONNECT", "SELECT"})

				By("revoking the SELECT privilege from the database user")
				_, err = runPostgresQuery(
					testNamespace,
					conn,
					`REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM "test-privileges-maintenance";`,
				)
				Expect(err).NotTo(HaveOccurred(), "Failed to revoke SELECT privilege")

				err = triggerReconciliation("test-privileges-maintenance", testNamespace)
				Expect(err).NotTo(HaveOccurred(), "Failed to trigger reconciliation after revoking privileges")

				By("verifying that the controller reconciles and restores the revoked privilege")
				waitForPrivilegesGranted(testNamespace, conn, "test-privileges-maintenance", []string{"CONNECT", "SELECT"})
			})

			It("should delete the database user when the PostgresAccess resource is deleted", func() {
				By("creating a PostgresAccess resource")
				testNamespace, conn := getDatabaseVariables()
				secretName, err := createConnectionDetailsViaSecret(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				err = createResourceFromSecretReference("test-deletion", testNamespace, "test-postgres-credentials-secret-ref", secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the privileges to be granted")
				waitForPrivilegesGranted(testNamespace, conn, "test-deletion", []string{"CONNECT", "SELECT"})

				By("deleting the PostgresAccess resource")
				err = deletePostgresAccess("test-deletion", testNamespace)
				Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

				By("verifying that the database user is deleted")
				waitForDatabaseUserState(testNamespace, conn, "test-deletion", false)
			})

			It("should delete the database user and secrets when the PostgresAccess resource is deleted", func() {
				By("creating a PostgresAccess resource")
				testNamespace, conn := getDatabaseVariables()
				secretName, err := createConnectionDetailsViaSecret(testNamespace, conn)
				genSecName := "test-postgres-credentials-secret-ref"
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				err = createResourceFromSecretReference("test-deletion", testNamespace, genSecName, secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the privileges to be granted")
				waitForPrivilegesGranted(testNamespace, conn, "test-deletion", []string{"CONNECT", "SELECT"})

				By("deleting the PostgresAccess resource")
				err = deletePostgresAccess("test-deletion", testNamespace)
				Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

				By("verifying that the database user is deleted")
				waitForDatabaseUserState(testNamespace, conn, "test-deletion", false)

				By("verifying that the generated secret is deleted")
				waitForSecretDeleted(testNamespace, genSecName)
			})

			It("should reassign owned objects to the database owner when cleanupPolicy is Orphan", func() {
				By("creating a PostgresAccess resource with cleanupPolicy Orphan")
				testNamespace, conn := getDatabaseVariables()
				secretName, err := createConnectionDetailsViaSecret(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				managedUsername := "test-orphan-cleanup"
				generatedSecret := "test-orphan-cleanup-credentials"
				orphanPolicy := accessv1.CleanupPolicyOrphan
				err = createResourceFromSecretReference(
					managedUsername,
					testNamespace,
					generatedSecret,
					secretName,
					&orphanPolicy,
					accessv1.GrantSpec{
						Database:   conn.Database,
						Privileges: []string{"CONNECT", "USAGE", "CREATE"},
					},
				)
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess with cleanupPolicy Orphan")

				By("waiting for the generated secret to be created and reading the managed password")
				managedPassword := waitForDecodedSecretField(testNamespace, generatedSecret, "password")

				By("waiting for the managed user to be created")
				waitForDatabaseUserState(testNamespace, conn, managedUsername, true)

				By("creating an object owned by the managed user")
				managedConn := conn
				managedConn.Username = managedUsername
				managedConn.Password = managedPassword
				ownedTable := "orphan_policy_owned_table"
				_, err = runPostgresQuery(
					testNamespace,
					managedConn,
					fmt.Sprintf(`CREATE TABLE public.%s (id SERIAL PRIMARY KEY, value TEXT);`, ownedTable),
				)
				Expect(err).NotTo(HaveOccurred(), "Failed to create an owned object as the managed user")

				By("verifying the object is initially owned by the managed user")
				waitForTableOwner(testNamespace, conn, ownedTable, managedUsername)

				By("deleting the PostgresAccess resource")
				err = deletePostgresAccess(managedUsername, testNamespace)
				Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

				By("verifying that the managed role is deleted")
				waitForDatabaseUserState(testNamespace, conn, managedUsername, false)

				By("verifying ownership is reassigned to the current database owner")
				waitForTableOwner(testNamespace, conn, ownedTable, conn.Username)
			})

			It("should update the database user's password when the PostgresAccess resource is updated with a new password", func() {
				By("creating a PostgresAccess resource")
				testNamespace, conn := getDatabaseVariables()
				secretName, err := createConnectionDetailsViaSecret(testNamespace, conn)
				genSecName := "test-postgres-credentials-secret-ref"
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				err = createResourceFromSecretReference("test-password-update", testNamespace, genSecName, secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the privileges to be granted")
				CRUsername := "test-password-update"
				waitForPrivilegesGranted(testNamespace, conn, CRUsername, []string{"CONNECT", "SELECT"})

				By("updating the PostgresAccess resource with a new password")
				// get the current password from the user with username test-password-update and update it in the YAML
				newPassword := "new-secure-password"
				updatedSecretYAML := fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
type: Opaque
data:
  username: %s
  password: %s
`, genSecName, testNamespace, b64.StdEncoding.EncodeToString([]byte(CRUsername)), b64.StdEncoding.EncodeToString([]byte(newPassword)))

				cmd := exec.Command("kubectl", "apply", "-f", "-")
				cmd.Stdin = strings.NewReader(updatedSecretYAML)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to update connection secret with new password")

				By("verifying that the database user's password is updated and the user can authenticate with the new password")
				waitForAuthenticationSuccess(testNamespace, conn, CRUsername, newPassword)
			})

			It("should update the database user's password the secret's password is rolled via deletion", func() {
				By("creating a PostgresAccess resource")
				testNamespace, conn := getDatabaseVariables()
				secretName, err := createConnectionDetailsViaSecret(testNamespace, conn)
				genSecName := "test-postgres-credentials-secret-ref"
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				CRUsername := "test-password-rotation"
				err = createResourceFromSecretReference(CRUsername, testNamespace, genSecName, secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the privileges to be granted")
				waitForPrivilegesGranted(testNamespace, conn, "test-password-rotation", []string{"CONNECT", "SELECT"})

				By("deleting the secret to trigger password rotation")
				cmd := exec.Command("kubectl", "delete", "secret", genSecName, "-n", testNamespace)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to delete connection secret")

				By("verifying that the database user's password is updated and the user can authenticate with the new password")
				newPassword := waitForDecodedSecretField(testNamespace, genSecName, "password")
				waitForAuthenticationSuccess(testNamespace, conn, CRUsername, newPassword)
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

func createPostgresAccessWithDirectConnection(
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
	_, err := utils.Run(cmd)
	return err
}

func createResourceFromSecretReference(
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
	_, err := utils.Run(cmd)
	return err
}

func formatPrivilegesYAML(privileges []string, indent string) string {
	lines := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		lines = append(lines, fmt.Sprintf("%s- %s", indent, privilege))
	}
	return strings.Join(lines, "\n")
}

func waitForSecretField(namespace, secretName, field string) string {
	var output string
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "secret", secretName, "-n", namespace, "-o", fmt.Sprintf("jsonpath={.data.%s}", field))
		var err error
		output, err = utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve secret field")
		g.Expect(strings.TrimSpace(output)).NotTo(BeEmpty(), "Secret field should not be empty")
	}, 2*time.Minute, 5*time.Second).Should(Succeed())

	return strings.TrimSpace(output)
}

func waitForDecodedSecretField(namespace, secretName, field string) string {
	encoded := waitForSecretField(namespace, secretName, field)
	decoded, err := b64.StdEncoding.DecodeString(encoded)
	Expect(err).NotTo(HaveOccurred(), "Secret value should be valid base64")
	return string(decoded)
}

func waitForDatabaseUserState(
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
		output, err := runPostgresQuery(
			namespace,
			connection,
			fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = '%s');", username),
		)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to check if user exists")
		g.Expect(output).To(Equal(expected))
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

func waitForPrivilegesGranted(
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

func waitForTableOwner(namespace string, connection controller.ConnectionDetails, table, expectedOwner string) {
	Eventually(func(g Gomega) {
		output, err := runPostgresQuery(
			namespace,
			connection,
			fmt.Sprintf("SELECT tableowner FROM pg_tables WHERE schemaname = 'public' AND tablename = '%s';", table),
		)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to query table owner")
		g.Expect(output).To(Equal(expectedOwner))
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

func waitForAuthenticationSuccess(
	namespace string,
	connection controller.ConnectionDetails,
	username,
	password string,
) {
	updatedConn := connection
	updatedConn.Username = username
	updatedConn.Password = password

	Eventually(func(g Gomega) {
		output, err := runPostgresQuery(
			namespace,
			updatedConn,
			"SELECT 1;",
		)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to authenticate with the expected credentials")
		g.Expect(output).To(Equal("1"), "PostgreSQL should return a valid query result")
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

func deletePostgresAccess(name, namespace string) error {
	cmd := exec.Command("kubectl", "delete", "postgresaccess", name, "-n", namespace)
	_, err := utils.Run(cmd)
	return err
}

func waitForSecretDeleted(namespace, secretName string) {
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "secret", secretName, "-n", namespace, "-o", "name", "--ignore-not-found")
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to check if secret exists")
		g.Expect(strings.TrimSpace(output)).To(BeEmpty(), "Secret should have been deleted")
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
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

func triggerReconciliation(resourceName, namespace string) error {
	timestamp := time.Now().Format(time.RFC3339Nano)
	cmd := exec.Command("kubectl", "annotate", "postgresaccess", resourceName,
		"-n", namespace,
		fmt.Sprintf("reconcile-trigger=%s", timestamp),
		"--overwrite")
	_, err := utils.Run(cmd)
	return err
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
