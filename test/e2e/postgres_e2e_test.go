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
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/delta10/access-operator/api/v1"
	"github.com/delta10/access-operator/test/utils"
)

var _ = Describe("Postgres", Ordered, func() {
	It("should log reconcile errors and set Ready=False when connection details are invalid", func() {
		testNamespace := "postgres-access-error-test"
		resourceName := "invalid-connection"
		generatedSecretName := "invalid-connection-secret"

		By("resetting the test namespace")
		cmd := exec.Command("kubectl", "delete", "ns", testNamespace, "--ignore-not-found", "--timeout=1m")
		_, _ = utils.Run(cmd)

		cmd = exec.Command("kubectl", "create", "ns", testNamespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create test namespace")

		DeferCleanup(func() {
			cleanupCmd := exec.Command("kubectl", "delete", "ns", testNamespace, "--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cleanupCmd)
		})

		By("creating a PostgresAccess resource with invalid connection details")
		invalidResource := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: PostgresAccess
metadata:
  name: %s
  namespace: %s
spec:
  generatedSecret: %s
  username: %s
  connection: {}
  grants:
    - database: postgres
      privileges:
        - CONNECT
`, resourceName, testNamespace, generatedSecretName, resourceName)

		cmd = exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(invalidResource)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create invalid PostgresAccess resource")

		By("verifying the PostgresAccess status reports the reconcile failure")
		Eventually(func(g Gomega) {
			statusCmd := exec.Command(
				"kubectl", "get", "postgresaccess", resourceName, "-n", testNamespace,
				"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
			)
			statusOutput, statusErr := utils.Run(statusCmd)
			g.Expect(statusErr).NotTo(HaveOccurred(), "Failed to retrieve Ready condition status")
			g.Expect(strings.TrimSpace(statusOutput)).To(Equal("False"))

			reasonCmd := exec.Command(
				"kubectl", "get", "postgresaccess", resourceName, "-n", testNamespace,
				"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}",
			)
			reasonOutput, reasonErr := utils.Run(reasonCmd)
			g.Expect(reasonErr).NotTo(HaveOccurred(), "Failed to retrieve Ready condition reason")
			g.Expect(strings.TrimSpace(reasonOutput)).To(Equal("DatabaseSyncFailed"))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying the controller logs the expected reconcile error")
		Eventually(func(g Gomega) {
			controllerPodName = ensureControllerPodName()
			logCmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace, "--since=10m")
			logOutput, logErr := utils.Run(logCmd)
			g.Expect(logErr).NotTo(HaveOccurred(), "Failed to read controller logs")
			g.Expect(logOutput).To(ContainSubstring(resourceName))
			g.Expect(logOutput).To(ContainSubstring("failed to reconcile PostgresAccess"))
			g.Expect(logOutput).To(ContainSubstring("no valid connection details provided"))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())
	})

	Context("CNPG", func() {
		BeforeAll(func() {
			By("deploying a PGSQL instance for testing")
			testNamespace := "pgsql-test"
			cmd := exec.Command("kubectl", "create", "ns", testNamespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create namespace for PGSQL testing")

			err = utils.DeployCNPGInstance(testNamespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to deploy PGSQL instance")

			By("waiting for CNPG to accept SQL connections")
			conn := utils.GetCNPGConnectionDetailsFromSecret(testNamespace, "cnpg-postgres-app")
			utils.WaitForAuthenticationSuccess(testNamespace, conn, conn.Username, conn.Password)
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
			testNamespace := fmt.Sprintf("postgres-access-test-%d", time.Now().UnixNano()%1_000_000)
			Expect(os.Setenv("POSTGRES_TEST_NAMESPACE", testNamespace)).To(Succeed())

			By("creating a fresh test namespace")
			cmd := exec.Command("kubectl", "create", "ns", testNamespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create test namespace")

			_, conn := utils.GetDatabaseVariables()

			By("ensuring singleton Controller resources are cleaned up before each postgres test")
			cmd = exec.Command("kubectl", "delete", "controller", "--all", "-n", namespace, "--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)

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

			Expect(os.Unsetenv("POSTGRES_TEST_NAMESPACE")).To(Succeed())
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

		It("should deny cross-namespace existingSecret when no Controller resource exists", func() {
			testNamespace, conn := utils.GetDatabaseVariables()
			connectionSecretNamespace := "postgres-access-shared-no-controller"
			resourceName := "test-cross-namespace-no-controller"
			generatedSecretName := "test-cross-namespace-no-controller-secret"

			By("creating a shared namespace for the connection secret")
			cmd := exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--timeout=1m")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "create", "ns", connectionSecretNamespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create shared secret namespace")
			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating the connection secret in another namespace")
			secretName, err := utils.CreateConnectionDetailsViaSecret(connectionSecretNamespace, conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = utils.CreateResourceFromSecretReferenceWithNamespace(
				resourceName,
				testNamespace,
				generatedSecretName,
				secretName,
				connectionSecretNamespace,
				nil,
				accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace PostgresAccess")

			By("verifying reconcile is denied with cross-namespace policy disabled")
			Eventually(func(g Gomega) {
				statusCmd := exec.Command(
					"kubectl", "get", "postgresaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
				)
				statusOutput, statusErr := utils.Run(statusCmd)
				g.Expect(statusErr).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(statusOutput)).To(Equal("False"))

				reasonCmd := exec.Command(
					"kubectl", "get", "postgresaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}",
				)
				reasonOutput, reasonErr := utils.Run(reasonCmd)
				g.Expect(reasonErr).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(reasonOutput)).To(Equal("DatabaseSyncFailed"))

				messageCmd := exec.Command(
					"kubectl", "get", "postgresaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].message}",
				)
				messageOutput, messageErr := utils.Run(messageCmd)
				g.Expect(messageErr).NotTo(HaveOccurred())
				g.Expect(messageOutput).To(ContainSubstring("cross-namespace connection secret references are disabled"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the requested database user was not created")
			utils.WaitForDatabaseUserState(testNamespace, conn, resourceName, false)
		})

		It("should deny cross-namespace existingSecret when singleton Controller setting is false", func() {
			testNamespace, conn := utils.GetDatabaseVariables()
			connectionSecretNamespace := "postgres-access-shared-controller-false"
			controllerName := "cluster-settings-false"
			resourceName := "test-cross-namespace-controller-false"
			generatedSecretName := "test-cross-namespace-controller-false-secret"

			By("creating a singleton Controller with existingSecretNamespace=false")
			controllerYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: Controller
metadata:
  name: %s
  namespace: %s
spec:
  settings:
    existingSecretNamespace: false
`, controllerName, namespace)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(controllerYAML)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create singleton Controller with false policy")
			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "controller", controllerName, "-n", namespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating a shared namespace for the connection secret")
			cmd = exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--timeout=1m")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "create", "ns", connectionSecretNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create shared secret namespace")
			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating the connection secret in another namespace")
			secretName, err := utils.CreateConnectionDetailsViaSecret(connectionSecretNamespace, conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = utils.CreateResourceFromSecretReferenceWithNamespace(
				resourceName,
				testNamespace,
				generatedSecretName,
				secretName,
				connectionSecretNamespace,
				nil,
				accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace PostgresAccess")

			By("verifying reconcile is denied because singleton Controller policy is false")
			Eventually(func(g Gomega) {
				messageCmd := exec.Command(
					"kubectl", "get", "postgresaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].message}",
				)
				messageOutput, messageErr := utils.Run(messageCmd)
				g.Expect(messageErr).NotTo(HaveOccurred())
				g.Expect(messageOutput).To(ContainSubstring("cross-namespace connection secret references are disabled"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should create a PostgresAccess resource using an existing connection secret from another namespace", func() {
			testNamespace, conn := utils.GetDatabaseVariables()
			connectionSecretNamespace := fmt.Sprintf("%s-shared", testNamespace)
			controllerName := "cluster-settings"
			var secretName string

			By("enabling cross-namespace references through the singleton Controller resource")
			controllerYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: Controller
metadata:
  name: %s
  namespace: %s
spec:
  settings:
    existingSecretNamespace: true
`, controllerName, namespace)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(controllerYAML)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to enable cross-namespace references via Controller CR")

			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "controller", controllerName, "-n", namespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("resetting the shared secret namespace used for cross-namespace secret references")
			cmd = exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--timeout=1m")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to clean up existing shared secret namespace")

			cmd = exec.Command("kubectl", "create", "ns", connectionSecretNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create shared secret namespace")

			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating the connection secret in the shared namespace")
			secretName, err = utils.CreateConnectionDetailsViaSecret(connectionSecretNamespace, conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess resource in the workload namespace that references the shared secret")
			err = utils.CreateResourceFromSecretReferenceWithNamespace(
				"test-username-cross-namespace",
				testNamespace,
				"test-postgres-credentials-cross-namespace",
				secretName,
				connectionSecretNamespace,
				nil,
				accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with cross-namespace secret reference")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(testNamespace, "test-postgres-credentials-cross-namespace", "username")

			By("verifying the database user was created")
			utils.WaitForDatabaseUserState(testNamespace, conn, "test-username-cross-namespace", true)
		})

		It("should fail when multiple Controller resources exist and emit warning events", func() {
			testNamespace, conn := utils.GetDatabaseVariables()
			connectionSecretNamespace := "postgres-access-shared-multiple-controller"
			controllerAName := "cluster-settings-a"
			controllerBName := "cluster-settings-b"
			resourceName := "test-cross-namespace-multiple-controllers"
			generatedSecretName := "test-cross-namespace-multiple-controllers-secret"

			By("creating two Controller resources to violate singleton policy")
			controllerAYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: Controller
metadata:
  name: %s
  namespace: %s
spec:
  settings:
    existingSecretNamespace: true
`, controllerAName, namespace)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(controllerAYAML)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create first Controller")

			controllerBYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: Controller
metadata:
  name: %s
  namespace: %s
spec:
  settings:
    existingSecretNamespace: true
`, controllerBName, testNamespace)
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(controllerBYAML)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create second Controller")

			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "controller", controllerAName, "-n", namespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating a shared namespace for the connection secret")
			cmd = exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--timeout=1m")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "create", "ns", connectionSecretNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create shared secret namespace")
			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating the connection secret in another namespace")
			secretName, err := utils.CreateConnectionDetailsViaSecret(connectionSecretNamespace, conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = utils.CreateResourceFromSecretReferenceWithNamespace(
				resourceName,
				testNamespace,
				generatedSecretName,
				secretName,
				connectionSecretNamespace,
				nil,
				accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace PostgresAccess")

			By("verifying PostgresAccess fails with multiple-controller error")
			Eventually(func(g Gomega) {
				reasonCmd := exec.Command(
					"kubectl", "get", "postgresaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}",
				)
				reasonOutput, reasonErr := utils.Run(reasonCmd)
				g.Expect(reasonErr).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(reasonOutput)).To(Equal("DatabaseSyncFailed"))

				messageCmd := exec.Command(
					"kubectl", "get", "postgresaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].message}",
				)
				messageOutput, messageErr := utils.Run(messageCmd)
				g.Expect(messageErr).NotTo(HaveOccurred())
				g.Expect(messageOutput).To(ContainSubstring("multiple Controller resources found"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying both Controller resources are marked Ready=False with MultipleControllersFound")
			Eventually(func(g Gomega) {
				for _, key := range []struct {
					name      string
					namespace string
				}{
					{name: controllerAName, namespace: namespace},
					{name: controllerBName, namespace: testNamespace},
				} {
					statusCmd := exec.Command(
						"kubectl", "get", "controller", key.name, "-n", key.namespace,
						"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
					)
					statusOutput, statusErr := utils.Run(statusCmd)
					g.Expect(statusErr).NotTo(HaveOccurred())
					g.Expect(strings.TrimSpace(statusOutput)).To(Equal("False"))

					reasonCmd := exec.Command(
						"kubectl", "get", "controller", key.name, "-n", key.namespace,
						"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}",
					)
					reasonOutput, reasonErr := utils.Run(reasonCmd)
					g.Expect(reasonErr).NotTo(HaveOccurred())
					g.Expect(strings.TrimSpace(reasonOutput)).To(Equal("MultipleControllersFound"))
				}
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying warning events are emitted for both Controller resources")
			Eventually(func(g Gomega) {
				for _, key := range []struct {
					name      string
					namespace string
				}{
					{name: controllerAName, namespace: namespace},
					{name: controllerBName, namespace: testNamespace},
				} {
					eventsCmd := exec.Command(
						"kubectl", "get", "events", "-n", key.namespace,
						"--field-selector",
						fmt.Sprintf("involvedObject.kind=Controller,involvedObject.name=%s,reason=MultipleControllersFound", key.name),
						"--no-headers",
					)
					eventsOutput, eventsErr := utils.Run(eventsCmd)
					g.Expect(eventsErr).NotTo(HaveOccurred())
					g.Expect(strings.TrimSpace(eventsOutput)).NotTo(BeEmpty())
				}
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying warning event is emitted on controller-manager Deployment")
			Eventually(func(g Gomega) {
				eventsCmd := exec.Command(
					"kubectl", "get", "events", "-n", namespace,
					"--field-selector",
					fmt.Sprintf("involvedObject.kind=Deployment,involvedObject.name=%s,reason=MultipleControllersFound", managerDeploymentName),
					"--no-headers",
				)
				eventsOutput, eventsErr := utils.Run(eventsCmd)
				g.Expect(eventsErr).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(eventsOutput)).NotTo(BeEmpty())
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
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

			err = utils.TriggerReconciliation("postgresaccess", "test-privileges-maintenance", testNamespace)
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

		It("should preserve excluded PostgreSQL users from singleton Controller settings", func() {
			testNamespace, conn := utils.GetDatabaseVariables()
			excludedUsername := "excluded-keeper"
			managedUsername := "test-managed-user"
			controllerName := "cluster-settings-excluded-users"

			By("creating a singleton Controller with excluded PostgreSQL users")
			controllerYAML := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: Controller
metadata:
  name: %s
  namespace: %s
spec:
  settings:
    postgres:
      excludedUsers:
        - %s
`, controllerName, namespace, excludedUsername)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(controllerYAML)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create singleton Controller with excluded users")
			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "controller", controllerName, "-n", namespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating an unmanaged PostgreSQL role that should be preserved")
			_, err = utils.RunPostgresQuery(
				testNamespace,
				conn,
				fmt.Sprintf(`CREATE ROLE "%s" WITH LOGIN PASSWORD 'keep-me';`, excludedUsername),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create excluded PostgreSQL role")

			By("creating a PostgresAccess resource to trigger reconciliation")
			secretName, err := utils.CreateConnectionDetailsViaSecret(testNamespace, conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils.CreateResourceFromSecretReference(
				managedUsername,
				testNamespace,
				"test-excluded-user-secret",
				secretName,
				nil,
				accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource")

			By("verifying the managed role is created")
			utils.WaitForDatabaseUserState(testNamespace, conn, managedUsername, true)

			By("verifying the excluded unmanaged role is not removed")
			utils.WaitForDatabaseUserState(testNamespace, conn, excludedUsername, true)
		})
	})
})
