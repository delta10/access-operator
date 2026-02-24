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
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/delta10/access-operator/api/v1"
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
			token, err := utils.ServiceAccountToken(namespace, serviceAccountName)
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
				metricsOutput, err := utils.GetMetricsOutput(namespace, "curl-metrics")
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		Context("CPPG", func() {
			BeforeAll(func() {
				By("deploying a PGSQL instance for testing")
				testNamespace := "pgsql-test"
				cmd := exec.Command("kubectl", "create", "ns", testNamespace)
				_, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to create namespace for PGSQL testing")

				err = utils.DeployCNPGInstance(testNamespace)
				Expect(err).NotTo(HaveOccurred(), "Failed to deploy PGSQL instance")
			})

			AfterAll(func() {
				By("cleaning up the PGSQL test namespace")
				cmd := exec.Command("kubectl", "delete", "ns", "pgsql-test", "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cmd)
			})

			It("should create a PostgresAccess resource and create a database user with the specified privileges on a CNPG instance", func() {
				testNamespace := "pgsql-test"
				conn := utils.GetCNPGConnectionDetailsFromSecret(testNamespace, "cnpg-postgres-app")

				By("creating a PostgresAccess resource referencing the connection secret")
				err := utils.CreateResourceFromSecretReference("test-username", testNamespace, "test-postgres-credentials-secret-ref", "cnpg-postgres-app", nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the generated secret to be created")
				utils.WaitForSecretField(testNamespace, "test-postgres-credentials-secret-ref", "username")

				By("verifying the database user was created")
				utils.WaitForDatabaseUserState(testNamespace, conn, "test-username", true)
			})
		})

		Context("Postgres", func() {
			BeforeEach(func() {
				By("ensuring no stale test namespace exists")
				testNamespace, conn := utils.GetDatabaseVariables()
				cmd := exec.Command("kubectl", "delete", "ns", testNamespace, "--ignore-not-found", "--timeout=1m")
				_, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to clean up existing test namespace")

				By("creating a fresh test namespace")
				cmd = exec.Command("kubectl", "create", "ns", testNamespace)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to create test namespace")

				By("deploying PostgreSQL in the test namespace")
				err = utils.DeployPostgresInstance(testNamespace, conn)
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
					output, err := utils.RunPostgresQuery(
						testNamespace,
						conn,
						"SELECT 1;",
					)
					g.Expect(err).NotTo(HaveOccurred(), "PostgreSQL should accept connections")
					g.Expect(output).To(Equal("1"), "PostgreSQL should return a valid query result")
				}
				Eventually(verifyPostgresReady, 2*time.Minute, 5*time.Second).Should(Succeed())

				By("ensuring the test table exists")
				_, err = utils.RunPostgresQuery(
					testNamespace,
					conn,
					"CREATE TABLE IF NOT EXISTS public.access_operator_test(id SERIAL PRIMARY KEY, value TEXT);",
				)
				Expect(err).NotTo(HaveOccurred(), "Failed to prepare test table")

				Expect(conn.Host).NotTo(BeEmpty(), "Postgres host should be configured")
			})

			AfterEach(func() {
				testNamespace, _ := utils.GetDatabaseVariables()

				By("cleaning up any remaining PostgresAccess resources")
				cmd := exec.Command("kubectl", "delete", "postgresaccess", "--all", "-n", testNamespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cmd)

				By("cleaning up the test namespace")
				cmd = exec.Command("kubectl", "delete", "ns", testNamespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cmd)
			})

			It("should create a PostgresAccess resource and create a database user with the specified privileges via direct connection details", func() {
				testNamespace, conn := utils.GetDatabaseVariables()
				resourceName := "test-postgres-access"
				generatedSecret := "test-postgres-credentials"

				By("creating a PostgresAccess resource")
				err := utils.CreatePostgresAccessWithDirectConnection(
					resourceName,
					testNamespace,
					generatedSecret,
					conn,
					[]string{"CONNECT", "SELECT"},
				)
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with connection details")

				By("waiting for the generated secret to be created")
				utils.WaitForSecretField(testNamespace, generatedSecret, "username")

				By("verifying the database user was created")
				utils.WaitForDatabaseUserState(testNamespace, conn, resourceName, true)

				By("verifying the privileges were granted")
				utils.WaitForPrivilegesGranted(testNamespace, conn, resourceName, []string{"CONNECT", "SELECT"})
			})

			It("should create a PostgresAccess resource with connectivity as a secret reference and create a database user accordingly", func() {
				By("creating a secret with the connection details")
				testNamespace, conn := utils.GetDatabaseVariables()
				secretName, err := utils.CreateConnectionDetailsViaSecret(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				By("creating a PostgresAccess resource referencing the connection secret")
				err = utils.CreateResourceFromSecretReference("test-username", testNamespace, "test-postgres-credentials-secret-ref", secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the generated secret to be created")
				utils.WaitForSecretField(testNamespace, "test-postgres-credentials-secret-ref", "username")

				By("verifying the database user was created")
				utils.WaitForDatabaseUserState(testNamespace, conn, "test-username", true)
			})

			It("should create a database user when connection is provided via direct connection details but user and pass via secret reference", func() {
				By("creating a secret with the username and password")
				testNamespace, conn := utils.GetDatabaseVariables()
				secretName, err := utils.CreateConnectionDetailsViaSecret(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				By("creating a PostgresAccess resource referencing the username/password secret and providing connection details directly")
				err = utils.CreatePostgresAccessWithConnectionSecretRef("test-user-pass", testNamespace, "test-user-pass-secret", conn, secretName, []string{"CONNECT", "SELECT"})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference for username/password")

				By("waiting for the generated secret to be created")
				utils.WaitForSecretField(testNamespace, "test-user-pass-secret", "username")

				By("verifying the database user was created")
				utils.WaitForDatabaseUserState(testNamespace, conn, "test-user-pass", true)
			})

			It("should reconcile privileges when they're changed in the config", func() {
				By("creating a PostgresAccess resource with certain privileges")
				testNamespace, conn := utils.GetDatabaseVariables()
				secretName, err := utils.CreateConnectionDetailsViaSecret(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				err = utils.CreateResourceFromSecretReference("test-privileges-reconciliation", testNamespace, "test-postgres-credentials-secret-ref", secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the initial privileges to be granted")
				utils.WaitForPrivilegesGranted(testNamespace, conn, "test-privileges-reconciliation", []string{"CONNECT"})

				By("updating the PostgresAccess resource to include additional privileges")
				err = utils.CreateResourceFromSecretReference("test-privileges-reconciliation", testNamespace, "test-postgres-credentials-secret-ref", secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT", "INSERT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to update PostgresAccess resource with new privileges")

				By("verifying that the new privileges are granted")
				utils.WaitForPrivilegesGranted(testNamespace, conn, "test-privileges-reconciliation", []string{"CONNECT", "SELECT", "INSERT"})
			})

			It("should reconcile the privileges of a PostgresAccess resource when they are manually revoked in the database", func() {
				By("creating a PostgresAccess resource")
				testNamespace, conn := utils.GetDatabaseVariables()
				secretName, err := utils.CreateConnectionDetailsViaSecret(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				err = utils.CreateResourceFromSecretReference("test-privileges-maintenance", testNamespace, "test-postgres-credentials-secret-ref", secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the privileges to be granted")
				utils.WaitForPrivilegesGranted(testNamespace, conn, "test-privileges-maintenance", []string{"CONNECT", "SELECT"})

				By("revoking the SELECT privilege from the database user")
				_, err = utils.RunPostgresQuery(
					testNamespace,
					conn,
					`REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM "test-privileges-maintenance";`,
				)
				Expect(err).NotTo(HaveOccurred(), "Failed to revoke SELECT privilege")

				err = utils.TriggerReconciliation("test-privileges-maintenance", testNamespace)
				Expect(err).NotTo(HaveOccurred(), "Failed to trigger reconciliation after revoking privileges")

				By("verifying that the controller reconciles and restores the revoked privilege")
				utils.WaitForPrivilegesGranted(testNamespace, conn, "test-privileges-maintenance", []string{"CONNECT", "SELECT"})
			})

			It("should delete the database user when the PostgresAccess resource is deleted", func() {
				By("creating a PostgresAccess resource")
				testNamespace, conn := utils.GetDatabaseVariables()
				secretName, err := utils.CreateConnectionDetailsViaSecret(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				err = utils.CreateResourceFromSecretReference("test-deletion", testNamespace, "test-postgres-credentials-secret-ref", secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the privileges to be granted")
				utils.WaitForPrivilegesGranted(testNamespace, conn, "test-deletion", []string{"CONNECT", "SELECT"})

				By("deleting the PostgresAccess resource")
				err = utils.DeletePostgresAccess("test-deletion", testNamespace)
				Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

				By("verifying that the database user is deleted")
				utils.WaitForDatabaseUserState(testNamespace, conn, "test-deletion", false)
			})

			It("should delete the database user and secrets when the PostgresAccess resource is deleted", func() {
				By("creating a PostgresAccess resource")
				testNamespace, conn := utils.GetDatabaseVariables()
				secretName, err := utils.CreateConnectionDetailsViaSecret(testNamespace, conn)
				genSecName := "test-postgres-credentials-secret-ref"
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				err = utils.CreateResourceFromSecretReference("test-deletion", testNamespace, genSecName, secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the privileges to be granted")
				utils.WaitForPrivilegesGranted(testNamespace, conn, "test-deletion", []string{"CONNECT", "SELECT"})

				By("deleting the PostgresAccess resource")
				err = utils.DeletePostgresAccess("test-deletion", testNamespace)
				Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

				By("verifying that the database user is deleted")
				utils.WaitForDatabaseUserState(testNamespace, conn, "test-deletion", false)

				By("verifying that the generated secret is deleted")
				utils.WaitForSecretDeleted(testNamespace, genSecName)
			})

			It("should reassign owned objects to the database owner when cleanupPolicy is Orphan", func() {
				By("creating a PostgresAccess resource with cleanupPolicy Orphan")
				testNamespace, conn := utils.GetDatabaseVariables()
				secretName, err := utils.CreateConnectionDetailsViaSecret(testNamespace, conn)
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				managedUsername := "test-orphan-cleanup"
				generatedSecret := "test-orphan-cleanup-credentials"
				orphanPolicy := accessv1.CleanupPolicyOrphan
				err = utils.CreateResourceFromSecretReference(
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
				managedPassword := utils.WaitForDecodedSecretField(testNamespace, generatedSecret, "password")

				By("waiting for the managed user to be created")
				utils.WaitForDatabaseUserState(testNamespace, conn, managedUsername, true)

				By("creating an object owned by the managed user")
				managedConn := conn
				managedConn.Username = managedUsername
				managedConn.Password = managedPassword
				ownedTable := "orphan_policy_owned_table"
				_, err = utils.RunPostgresQuery(
					testNamespace,
					managedConn,
					fmt.Sprintf(`CREATE TABLE public.%s (id SERIAL PRIMARY KEY, value TEXT);`, ownedTable),
				)
				Expect(err).NotTo(HaveOccurred(), "Failed to create an owned object as the managed user")

				By("verifying the object is initially owned by the managed user")
				utils.WaitForTableOwner(testNamespace, conn, ownedTable, managedUsername)

				By("deleting the PostgresAccess resource")
				err = utils.DeletePostgresAccess(managedUsername, testNamespace)
				Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

				By("verifying that the managed role is deleted")
				utils.WaitForDatabaseUserState(testNamespace, conn, managedUsername, false)

				By("verifying ownership is reassigned to the current database owner")
				utils.WaitForTableOwner(testNamespace, conn, ownedTable, conn.Username)
			})

			It("should update the database user's password when the PostgresAccess resource is updated with a new password", func() {
				By("creating a PostgresAccess resource")
				testNamespace, conn := utils.GetDatabaseVariables()
				secretName, err := utils.CreateConnectionDetailsViaSecret(testNamespace, conn)
				genSecName := "test-postgres-credentials-secret-ref"
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				err = utils.CreateResourceFromSecretReference("test-password-update", testNamespace, genSecName, secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the privileges to be granted")
				CRUsername := "test-password-update"
				utils.WaitForPrivilegesGranted(testNamespace, conn, CRUsername, []string{"CONNECT", "SELECT"})

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
				utils.WaitForAuthenticationSuccess(testNamespace, conn, CRUsername, newPassword)
			})

			It("should update the database user's password the secret's password is rolled via deletion", func() {
				By("creating a PostgresAccess resource")
				testNamespace, conn := utils.GetDatabaseVariables()
				secretName, err := utils.CreateConnectionDetailsViaSecret(testNamespace, conn)
				genSecName := "test-postgres-credentials-secret-ref"
				Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

				CRUsername := "test-password-rotation"
				err = utils.CreateResourceFromSecretReference(CRUsername, testNamespace, genSecName, secretName, nil, accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				})
				Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

				By("waiting for the privileges to be granted")
				utils.WaitForPrivilegesGranted(testNamespace, conn, "test-password-rotation", []string{"CONNECT", "SELECT"})

				By("deleting the secret to trigger password rotation")
				cmd := exec.Command("kubectl", "delete", "secret", genSecName, "-n", testNamespace)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to delete connection secret")

				By("verifying that the database user's password is updated and the user can authenticate with the new password")
				newPassword := utils.WaitForDecodedSecretField(testNamespace, genSecName, "password")
				utils.WaitForAuthenticationSuccess(testNamespace, conn, CRUsername, newPassword)
			})
		})
	})
})
