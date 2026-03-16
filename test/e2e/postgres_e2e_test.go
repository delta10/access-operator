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

var _ = Describe("Postgres", func() {
	It("should log reconcile errors and set Ready=False when connection details are invalid", func() {
		env := newPostgresSpecEnv()
		DeferCleanup(env.cleanup)

		resourceName := env.name("invalid-connection")
		generatedSecretName := env.name("invalid-connection-secret")

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
`, resourceName, env.namespace, generatedSecretName, resourceName)

		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(invalidResource)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create invalid PostgresAccess resource")

		By("verifying the PostgresAccess status reports the reconcile failure")
		Eventually(func(g Gomega) {
			statusCmd := exec.Command(
				"kubectl", "get", "postgresaccess", resourceName, "-n", env.namespace,
				"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
			)
			statusOutput, statusErr := utils.Run(statusCmd)
			g.Expect(statusErr).NotTo(HaveOccurred(), "Failed to retrieve Ready condition status")
			g.Expect(strings.TrimSpace(statusOutput)).To(Equal("False"))

			reasonCmd := exec.Command(
				"kubectl", "get", "postgresaccess", resourceName, "-n", env.namespace,
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

	Context("CNPG", Serial, func() {
		It("should create a PostgresAccess resource and create a database user with the specified privileges on a CNPG instance", func() {
			testNamespace := createTestNamespace("cnpg-test")
			DeferCleanup(func() {
				deleteNamespace(testNamespace)
			})

			By("deploying a PGSQL instance for testing")
			err := utils.DeployCNPGInstance(testNamespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to deploy PGSQL instance")

			By("waiting for CNPG to accept SQL connections")
			conn := utils.GetCNPGConnectionDetailsFromSecret(testNamespace, "cnpg-postgres-app")
			utils.WaitForAuthenticationSuccess(testNamespace, conn, conn.Username, conn.Password)

			resourceName := fmt.Sprintf("test-username-%s", uniqueSuffix())
			generatedSecret := fmt.Sprintf("test-postgres-credentials-%s", uniqueSuffix())

			By("creating a PostgresAccess resource referencing the connection secret")
			err = utils.CreateResourceFromSecretReference(
				resourceName,
				testNamespace,
				generatedSecret,
				"cnpg-postgres-app",
				nil,
				accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(testNamespace, generatedSecret, "username")

			By("verifying the database user was created")
			utils.WaitForDatabaseUserState(testNamespace, conn, resourceName, true)
		})
	})

	Context("Postgres", func() {
		var env postgresSpecEnv

		BeforeEach(func() {
			env = newPostgresSpecEnv()
		})

		AfterEach(func() {
			env.cleanup()
		})

		It("should create a PostgresAccess resource and create a database user with the specified privileges via direct connection details", func() {
			resourceName := env.name("test-postgres-access")
			generatedSecret := env.name("test-postgres-credentials")

			By("creating a PostgresAccess resource")
			err := utils.CreatePostgresAccessWithDirectConnection(
				resourceName,
				env.namespace,
				generatedSecret,
				env.conn,
				[]string{"CONNECT", "SELECT"},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with connection details")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the database user was created")
			utils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)

			By("verifying the privileges were granted")
			utils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})
		})

		It("should create a PostgresAccess resource with connectivity as a secret reference and create a database user accordingly", func() {
			resourceName := env.name("test-username")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a secret with the connection details")
			secretName, err := utils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			By("creating a PostgresAccess resource referencing the connection secret")
			err = utils.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the database user was created")
			utils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)
		})

		It("should create a database user when connection is provided via direct connection details but user and pass via secret reference", func() {
			resourceName := env.name("test-user-pass")
			generatedSecret := env.name("test-user-pass-secret")

			By("creating a secret with the username and password")
			secretName, err := utils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			By("creating a PostgresAccess resource referencing the username/password secret and providing connection details directly")
			err = utils.CreatePostgresAccessWithConnectionSecretRef(
				resourceName,
				env.namespace,
				generatedSecret,
				env.conn,
				secretName,
				[]string{"CONNECT", "SELECT"},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference for username/password")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the database user was created")
			utils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)
		})

		It("should reconcile privileges when they're changed in the config", func() {
			resourceName := env.name("test-privileges-reconciliation")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource with certain privileges")
			secretName, err := utils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the initial privileges to be granted")
			utils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT"})

			By("updating the PostgresAccess resource to include additional privileges")
			err = utils.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT", "INSERT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to update PostgresAccess resource with new privileges")

			By("verifying that the new privileges are granted")
			utils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT", "INSERT"})
		})

		It("should reconcile the privileges of a PostgresAccess resource when they are manually revoked in the database", func() {
			resourceName := env.name("test-privileges-maintenance")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := utils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the privileges to be granted")
			utils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

			By("revoking the SELECT privilege from the database user")
			_, err = utils.RunPostgresQuery(
				env.backendNamespace,
				env.conn,
				fmt.Sprintf(`REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM "%s";`, resourceName),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to revoke SELECT privilege")

			err = utils.TriggerReconciliation("postgresaccess", resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to trigger reconciliation after revoking privileges")

			By("verifying that the controller reconciles and restores the revoked privilege")
			utils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})
		})

		It("should delete the database user when the PostgresAccess resource is deleted", func() {
			resourceName := env.name("test-deletion")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := utils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the privileges to be granted")
			utils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

			By("deleting the PostgresAccess resource")
			err = utils.DeletePostgresAccess(resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

			By("verifying that the database user is deleted")
			utils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, false)
		})

		It("should delete the database user and secrets when the PostgresAccess resource is deleted", func() {
			resourceName := env.name("test-deletion")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := utils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the privileges to be granted")
			utils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

			By("deleting the PostgresAccess resource")
			err = utils.DeletePostgresAccess(resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

			By("verifying that the database user is deleted")
			utils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, false)

			By("verifying that the generated secret is deleted")
			utils.WaitForSecretDeleted(env.namespace, generatedSecret)
		})

		It("should reassign owned objects to the database owner when cleanupPolicy is Orphan", func() {
			managedUsername := env.name("test-orphan-cleanup")
			generatedSecret := env.name("test-orphan-cleanup-credentials")
			ownedTable := env.name("orphan-policy-owned-table")

			By("creating a PostgresAccess resource with cleanupPolicy Orphan")
			secretName, err := utils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			orphanPolicy := accessv1.CleanupPolicyOrphan
			err = utils.CreateResourceFromSecretReference(
				managedUsername,
				env.namespace,
				generatedSecret,
				secretName,
				&orphanPolicy,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "USAGE", "CREATE"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess with cleanupPolicy Orphan")

			By("waiting for the generated secret to be created and reading the managed password")
			managedPassword := utils.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")

			By("waiting for the managed user to be created")
			utils.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, true)

			By("creating an object owned by the managed user")
			managedConn := env.conn
			managedConn.Username = managedUsername
			managedConn.Password = managedPassword
			_, err = utils.RunPostgresQuery(
				env.backendNamespace,
				managedConn,
				fmt.Sprintf(`CREATE TABLE public.%q (id SERIAL PRIMARY KEY, value TEXT);`, ownedTable),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create an owned object as the managed user")

			By("verifying the object is initially owned by the managed user")
			utils.WaitForTableOwner(env.backendNamespace, env.conn, ownedTable, managedUsername)

			By("deleting the PostgresAccess resource")
			err = utils.DeletePostgresAccess(managedUsername, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

			By("verifying that the managed role is deleted")
			utils.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, false)

			By("verifying ownership is reassigned to the current database owner")
			utils.WaitForTableOwner(env.backendNamespace, env.conn, ownedTable, env.conn.Username)
		})

		It("should update the database user's password when the PostgresAccess resource is updated with a new password", func() {
			resourceName := env.name("test-password-update")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := utils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the privileges to be granted")
			utils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

			By("updating the PostgresAccess generated secret with a new password")
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
`, generatedSecret, env.namespace, b64.StdEncoding.EncodeToString([]byte(resourceName)), b64.StdEncoding.EncodeToString([]byte(newPassword)))

			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(updatedSecretYAML)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to update generated secret with new password")

			By("verifying that the database user's password is updated and the user can authenticate with the new password")
			utils.WaitForAuthenticationSuccess(env.backendNamespace, env.conn, resourceName, newPassword)
		})

		It("should update the database user's password the secret's password is rolled via deletion", func() {
			resourceName := env.name("test-password-rotation")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := utils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the privileges to be granted")
			utils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

			By("deleting the secret to trigger password rotation")
			cmd := exec.Command("kubectl", "delete", "secret", generatedSecret, "-n", env.namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete generated secret")

			By("verifying that the database user's password is updated and the user can authenticate with the new password")
			newPassword := utils.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")
			utils.WaitForAuthenticationSuccess(env.backendNamespace, env.conn, resourceName, newPassword)
		})
	})

	Context("Controller policy", Serial, func() {
		var env postgresSpecEnv

		BeforeEach(func() {
			clearAllControllers()
			env = newPostgresSpecEnv()
		})

		AfterEach(func() {
			env.cleanup()
			clearAllControllers()
		})

		It("should deny cross-namespace existingSecret when no Controller resource exists", func() {
			resourceName := env.name("test-cross-namespace-no-controller")
			generatedSecret := env.name("test-cross-namespace-no-controller-secret")
			connectionSecretNamespace := createTestNamespace("postgres-shared-no-controller")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})

			By("creating the connection secret in another namespace")
			secretName, err := utils.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = utils.CreateResourceFromSecretReferenceWithNamespace(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				connectionSecretNamespace,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace PostgresAccess")

			By("verifying reconcile is denied with cross-namespace policy disabled")
			Eventually(func(g Gomega) {
				statusCmd := exec.Command(
					"kubectl", "get", "postgresaccess", resourceName, "-n", env.namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
				)
				statusOutput, statusErr := utils.Run(statusCmd)
				g.Expect(statusErr).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(statusOutput)).To(Equal("False"))

				reasonCmd := exec.Command(
					"kubectl", "get", "postgresaccess", resourceName, "-n", env.namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}",
				)
				reasonOutput, reasonErr := utils.Run(reasonCmd)
				g.Expect(reasonErr).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(reasonOutput)).To(Equal("DatabaseSyncFailed"))

				messageCmd := exec.Command(
					"kubectl", "get", "postgresaccess", resourceName, "-n", env.namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].message}",
				)
				messageOutput, messageErr := utils.Run(messageCmd)
				g.Expect(messageErr).NotTo(HaveOccurred())
				g.Expect(messageOutput).To(ContainSubstring("cross-namespace connection secret references are disabled"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the requested database user was not created")
			utils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, false)
		})

		It("should deny cross-namespace existingSecret when singleton Controller setting is false", func() {
			resourceName := env.name("test-cross-namespace-controller-false")
			generatedSecret := env.name("test-cross-namespace-controller-false-secret")
			controllerName := env.name("cluster-settings-false")
			connectionSecretNamespace := createTestNamespace("postgres-shared-controller-false")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})

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

			By("creating the connection secret in another namespace")
			secretName, err := utils.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = utils.CreateResourceFromSecretReferenceWithNamespace(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				connectionSecretNamespace,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace PostgresAccess")

			By("verifying reconcile is denied because singleton Controller policy is false")
			Eventually(func(g Gomega) {
				messageCmd := exec.Command(
					"kubectl", "get", "postgresaccess", resourceName, "-n", env.namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].message}",
				)
				messageOutput, messageErr := utils.Run(messageCmd)
				g.Expect(messageErr).NotTo(HaveOccurred())
				g.Expect(messageOutput).To(ContainSubstring("cross-namespace connection secret references are disabled"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the requested database user was not created")
			utils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, false)
		})

		It("should create a PostgresAccess resource using an existing connection secret from another namespace", func() {
			resourceName := env.name("test-username-cross-namespace")
			generatedSecret := env.name("test-postgres-credentials-cross-namespace")
			controllerName := env.name("cluster-settings")
			connectionSecretNamespace := createTestNamespace("postgres-shared")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})

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

			By("creating the connection secret in the shared namespace")
			secretName, err := utils.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess resource in the workload namespace that references the shared secret")
			err = utils.CreateResourceFromSecretReferenceWithNamespace(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				connectionSecretNamespace,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with cross-namespace secret reference")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the database user was created")
			utils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)
		})

		It("should fail when multiple Controller resources exist and emit warning events", func() {
			resourceName := env.name("test-cross-namespace-multiple-controllers")
			generatedSecret := env.name("test-cross-namespace-multiple-controllers-secret")
			controllerAName := env.name("cluster-settings-a")
			controllerBName := env.name("cluster-settings-b")
			connectionSecretNamespace := createTestNamespace("postgres-shared-multiple-controller")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})

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
`, controllerBName, env.namespace)
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(controllerBYAML)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create second Controller")

			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "controller", controllerAName, "-n", namespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})
			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "controller", controllerBName, "-n", env.namespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating the connection secret in another namespace")
			secretName, err := utils.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = utils.CreateResourceFromSecretReferenceWithNamespace(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				connectionSecretNamespace,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace PostgresAccess")

			By("verifying PostgresAccess fails with multiple-controller error")
			Eventually(func(g Gomega) {
				reasonCmd := exec.Command(
					"kubectl", "get", "postgresaccess", resourceName, "-n", env.namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}",
				)
				reasonOutput, reasonErr := utils.Run(reasonCmd)
				g.Expect(reasonErr).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(reasonOutput)).To(Equal("DatabaseSyncFailed"))

				messageCmd := exec.Command(
					"kubectl", "get", "postgresaccess", resourceName, "-n", env.namespace,
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
					{name: controllerBName, namespace: env.namespace},
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
					{name: controllerBName, namespace: env.namespace},
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

		It("should preserve excluded PostgreSQL users from singleton Controller settings", func() {
			excludedUsername := env.name("excluded-keeper")
			managedUsername := env.name("test-managed-user")
			generatedSecret := env.name("test-excluded-user-secret")
			controllerName := env.name("cluster-settings-excluded-users")

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
				env.backendNamespace,
				env.conn,
				fmt.Sprintf(`CREATE ROLE "%s" WITH LOGIN PASSWORD 'keep-me';`, excludedUsername),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create excluded PostgreSQL role")

			By("creating a PostgresAccess resource to trigger reconciliation")
			secretName, err := utils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils.CreateResourceFromSecretReference(
				managedUsername,
				env.namespace,
				generatedSecret,
				secretName,
				nil,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource")

			By("verifying the managed role is created")
			utils.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, true)

			By("verifying the excluded unmanaged role is not removed")
			utils.WaitForDatabaseUserState(env.backendNamespace, env.conn, excludedUsername, true)
		})
	})
})
