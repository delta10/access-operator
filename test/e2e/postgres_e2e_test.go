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

	utils2 "github.com/delta10/access-operator/test/e2e/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/delta10/access-operator/api/v1"
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

		err := utils2.ApplyManifest(invalidResource)
		Expect(err).NotTo(HaveOccurred(), "Failed to create invalid PostgresAccess resource")

		By("verifying the PostgresAccess status reports the reconcile failure")
		waitForReadyCondition("postgresaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
			status: "False",
			reason: "DatabaseSyncFailed",
		})

		By("verifying the controller logs the expected reconcile error")
		waitForControllerLogsContain(resourceName, "failed to reconcile managed access resource", "no valid connection details provided")
	})

	Context("CNPG", Serial, func() {
		It("should create a PostgresAccess resource and create a database user with the specified privileges on a CNPG instance", func() {
			testNamespace := createTestNamespace("cnpg-test")
			DeferCleanup(func() {
				deleteNamespace(testNamespace)
			})

			By("deploying a PGSQL instance for testing")
			err := utils2.DeployCNPGInstance(testNamespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to deploy PGSQL instance")

			By("waiting for CNPG to accept SQL connections")
			conn := utils2.GetCNPGConnectionDetailsFromSecret(testNamespace, "cnpg-postgres-app")
			utils2.WaitForAuthenticationSuccess(testNamespace, conn, conn.Username, conn.Password)

			resourceName := fmt.Sprintf("test-username-%s", uniqueSuffix())
			generatedSecret := fmt.Sprintf("test-postgres-credentials-%s", uniqueSuffix())

			By("creating a PostgresAccess resource referencing the connection secret")
			err = utils2.CreateResourceFromSecretReference(
				resourceName,
				testNamespace,
				generatedSecret,
				"cnpg-postgres-app",
				accessv1.GrantSpec{
					Database:   conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the generated secret to be created")
			utils2.WaitForSecretField(testNamespace, generatedSecret, "username")

			By("verifying the database user was created")
			utils2.WaitForDatabaseUserState(testNamespace, conn, resourceName, true)
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
			err := utils2.CreatePostgresAccessWithDirectConnection(
				resourceName,
				env.namespace,
				generatedSecret,
				env.conn,
				[]string{"CONNECT", "SELECT"},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with connection details")

			By("waiting for the generated secret to be created")
			utils2.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the database user was created")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)

			By("verifying the privileges were granted")
			utils2.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})
		})

		It("should create a PostgresAccess resource with connectivity as a secret reference and create a database user accordingly", func() {
			resourceName := env.name("test-username")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a secret with the connection details")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			By("creating a PostgresAccess resource referencing the connection secret")
			err = utils2.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the generated secret to be created")
			utils2.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the database user was created")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)
		})

		It("should create a database user when connection is provided via direct connection details but user and pass via secret reference", func() {
			resourceName := env.name("test-user-pass")
			generatedSecret := env.name("test-user-pass-secret")

			By("creating a secret with the username and password")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			By("creating a PostgresAccess resource referencing the username/password secret and providing connection details directly")
			err = utils2.CreatePostgresAccessWithConnectionSecretRef(
				resourceName,
				env.namespace,
				generatedSecret,
				env.conn,
				secretName,
				[]string{"CONNECT", "SELECT"},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference for username/password")

			By("waiting for the generated secret to be created")
			utils2.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the database user was created")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)
		})

		It("should reconcile privileges when they're changed in the config", func() {
			resourceName := env.name("test-privileges-reconciliation")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource with certain privileges")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils2.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the initial privileges to be granted")
			utils2.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT"})

			By("updating the PostgresAccess resource to include additional privileges")
			err = utils2.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT", "INSERT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to update PostgresAccess resource with new privileges")

			By("verifying that the new privileges are granted")
			utils2.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT", "INSERT"})
		})

		It("should reconcile the privileges of a PostgresAccess resource when they are manually revoked in the database", func() {
			resourceName := env.name("test-privileges-maintenance")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils2.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the privileges to be granted")
			utils2.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

			By("revoking the SELECT privilege from the database user")
			_, err = utils2.RunPostgresQuery(
				env.backendNamespace,
				env.conn,
				fmt.Sprintf(`REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM "%s";`, resourceName),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to revoke SELECT privilege")

			err = utils2.TriggerReconciliation("postgresaccess", resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to trigger reconciliation after revoking privileges")

			By("verifying that the controller reconciles and restores the revoked privilege")
			utils2.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})
		})

		It("should retain the database user and delete the generated secret when the PostgresAccess resource is deleted by default", func() {
			resourceName := env.name("test-deletion")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils2.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the privileges to be granted")
			utils2.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

			By("deleting the PostgresAccess resource")
			err = utils2.DeletePostgresAccess(resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

			By("verifying finalization removed the PostgresAccess resource")
			utils2.WaitForResourceDeleted("postgresaccess", resourceName, env.namespace)

			By("verifying that the database user is retained by default policy")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)

			By("verifying that the generated secret is deleted")
			utils2.WaitForSecretDeleted(env.namespace, generatedSecret)
		})

		It("should reassign owned objects to the database owner when stale user deletion policy is Orphan", Serial, func() {
			managedUsername := env.name("test-orphan-cleanup")
			generatedSecret := env.name("test-orphan-cleanup-credentials")
			ownedTable := env.name("orphan-policy-owned-table")
			controllerName := env.name("postgres-orphan-policy")

			By("creating a singleton Controller with staleUserDeletionPolicy Orphan")
			err := createControllerResource(controllerName, namespace, `postgres:
  staleUserDeletionPolicy: Orphan`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create singleton Controller with Orphan policy")
			DeferCleanup(func() {
				deleteControllerResource(controllerName, namespace)
			})

			By("creating a PostgresAccess resource")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils2.CreateResourceFromSecretReference(
				managedUsername,
				env.namespace,
				generatedSecret,
				secretName,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "USAGE", "CREATE"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess with Orphan controller policy")

			By("waiting for the generated secret to be created and reading the managed password")
			managedPassword := utils2.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")

			By("waiting for the managed user to be created")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, true)

			By("creating an object owned by the managed user")
			managedConn := env.conn
			managedConn.Username = managedUsername
			managedConn.Password = managedPassword
			_, err = utils2.RunPostgresQuery(
				env.backendNamespace,
				managedConn,
				fmt.Sprintf(`CREATE TABLE public.%q (id SERIAL PRIMARY KEY, value TEXT);`, ownedTable),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create an owned object as the managed user")

			By("verifying the object is initially owned by the managed user")
			utils2.WaitForTableOwner(env.backendNamespace, env.conn, ownedTable, managedUsername)

			By("deleting the PostgresAccess resource")
			err = utils2.DeletePostgresAccess(managedUsername, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

			By("verifying that the managed role is deleted")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, false)

			By("verifying ownership is reassigned to the current database owner")
			utils2.WaitForTableOwner(env.backendNamespace, env.conn, ownedTable, env.conn.Username)
		})

		It("should update the database user's password when the PostgresAccess resource is updated with a new password", func() {
			resourceName := env.name("test-password-update")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils2.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the privileges to be granted")
			utils2.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

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

			err = utils2.ApplyManifest(updatedSecretYAML)
			Expect(err).NotTo(HaveOccurred(), "Failed to update generated secret with new password")

			By("verifying that the database user's password is updated and the user can authenticate with the new password")
			utils2.WaitForAuthenticationSuccess(env.backendNamespace, env.conn, resourceName, newPassword)
		})

		It("should update the database user's password the secret's password is rolled via deletion", func() {
			resourceName := env.name("test-password-rotation")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils2.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference")

			By("waiting for the privileges to be granted")
			utils2.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

			By("deleting the secret to trigger password rotation")
			cmd := exec.Command("kubectl", "delete", "secret", generatedSecret, "-n", env.namespace)
			_, err = utils2.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete generated secret")

			By("verifying that the database user's password is updated and the user can authenticate with the new password")
			newPassword := utils2.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")
			utils2.WaitForAuthenticationSuccess(env.backendNamespace, env.conn, resourceName, newPassword)
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
			secretName, err := utils2.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = utils2.CreateResourceFromSecretReferenceWithNamespace(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				connectionSecretNamespace,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace PostgresAccess")

			By("verifying reconcile is denied with cross-namespace policy disabled")
			waitForReadyCondition("postgresaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				status:          "False",
				reason:          "DatabaseSyncFailed",
				messageContains: "cross-namespace connection secret references are disabled",
			})

			By("verifying the requested database user was not created")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, false)
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
			err := createControllerResource(controllerName, namespace, `existingSecretNamespace: false`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create singleton Controller with false policy")
			DeferCleanup(func() {
				deleteControllerResource(controllerName, namespace)
			})

			By("creating the connection secret in another namespace")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = utils2.CreateResourceFromSecretReferenceWithNamespace(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				connectionSecretNamespace,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace PostgresAccess")

			By("verifying reconcile is denied because singleton Controller policy is false")
			waitForReadyCondition("postgresaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				messageContains: "cross-namespace connection secret references are disabled",
			})

			By("verifying the requested database user was not created")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, false)
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
			err := createControllerResource(controllerName, namespace, `existingSecretNamespace: true`)
			Expect(err).NotTo(HaveOccurred(), "Failed to enable cross-namespace references via Controller CR")
			DeferCleanup(func() {
				deleteControllerResource(controllerName, namespace)
			})

			By("creating the connection secret in the shared namespace")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess resource in the workload namespace that references the shared secret")
			err = utils2.CreateResourceFromSecretReferenceWithNamespace(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				connectionSecretNamespace,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with cross-namespace secret reference")

			By("waiting for the generated secret to be created")
			utils2.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the database user was created")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)
		})

		It("should deny cross-namespace existingSecret when the singleton Controller is outside the operator namespace", func() {
			resourceName := env.name("test-cross-namespace-wrong-controller-namespace")
			generatedSecret := env.name("test-cross-namespace-wrong-controller-namespace-secret")
			controllerName := env.name("cluster-settings-wrong-namespace")
			connectionSecretNamespace := createTestNamespace("postgres-shared-wrong-controller-namespace")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})

			By("creating the singleton Controller in a workload namespace instead of the operator namespace")
			err := createControllerResource(controllerName, env.namespace, `existingSecretNamespace: true`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create singleton Controller outside the operator namespace")
			DeferCleanup(func() {
				deleteControllerResource(controllerName, env.namespace)
			})

			By("creating the connection secret in another namespace")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = utils2.CreateResourceFromSecretReferenceWithNamespace(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				connectionSecretNamespace,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace PostgresAccess")

			By("verifying reconcile is denied because the Controller is not in the operator namespace")
			waitForReadyCondition("postgresaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				status:          "False",
				reason:          "DatabaseSyncFailed",
				messageContains: `must be created in the operator namespace "access-operator-system"`,
			})

			By("verifying the misplaced Controller is marked not ready")
			waitForReadyCondition("controller", namespacedName{name: controllerName, namespace: env.namespace}, readyConditionExpectation{
				status:          "False",
				reason:          "InvalidControllerNamespace",
				messageContains: `must be created in the operator namespace "access-operator-system"`,
			})

			By("verifying the requested database user was not created")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, false)
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
			err := createControllerResource(controllerAName, namespace, `existingSecretNamespace: true`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create first Controller")

			err = createControllerResource(controllerBName, env.namespace, `existingSecretNamespace: true`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create second Controller")

			DeferCleanup(func() {
				deleteControllerResource(controllerAName, namespace)
			})
			DeferCleanup(func() {
				deleteControllerResource(controllerBName, env.namespace)
			})

			By("creating the connection secret in another namespace")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = utils2.CreateResourceFromSecretReferenceWithNamespace(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				connectionSecretNamespace,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace PostgresAccess")

			By("verifying PostgresAccess fails with multiple-controller error")
			waitForReadyCondition("postgresaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				reason:          "DatabaseSyncFailed",
				messageContains: "multiple Controller resources found",
			})

			By("verifying both Controller resources are marked Ready=False with MultipleControllersFound")
			controllerResources := []namespacedName{
				{name: controllerAName, namespace: namespace},
				{name: controllerBName, namespace: env.namespace},
			}
			waitForControllerResourcesReadyCondition(controllerResources, readyConditionExpectation{
				status: "False",
				reason: "MultipleControllersFound",
			})

			By("verifying warning events are emitted for both Controller resources")
			for _, resource := range controllerResources {
				waitForResourceWarningEvent(resource, "Controller", "MultipleControllersFound")
			}

			By("verifying warning event is emitted on controller-manager Deployment")
			waitForResourceWarningEvent(namespacedName{name: managerDeploymentName, namespace: namespace}, "Deployment", "MultipleControllersFound")
		})

		It("should preserve excluded PostgreSQL users from singleton Controller settings", func() {
			excludedUsername := env.name("excluded-keeper")
			managedUsername := env.name("test-managed-user")
			generatedSecret := env.name("test-excluded-user-secret")
			controllerName := env.name("cluster-settings-excluded-users")

			By("creating a singleton Controller with excluded PostgreSQL users")
			err := createControllerResource(controllerName, namespace, fmt.Sprintf(`postgres:
  excludedUsers:
    - %s`, excludedUsername))
			Expect(err).NotTo(HaveOccurred(), "Failed to create singleton Controller with excluded users")
			DeferCleanup(func() {
				deleteControllerResource(controllerName, namespace)
			})

			By("creating an unmanaged PostgreSQL role that should be preserved")
			_, err = utils2.RunPostgresQuery(
				env.backendNamespace,
				env.conn,
				fmt.Sprintf(`CREATE ROLE "%s" WITH LOGIN PASSWORD 'keep-me';`, excludedUsername),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create excluded PostgreSQL role")

			By("creating a PostgresAccess resource to trigger reconciliation")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils2.CreateResourceFromSecretReference(
				managedUsername,
				env.namespace,
				generatedSecret,
				secretName,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource")

			By("verifying the managed role is created")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, true)

			By("verifying the excluded unmanaged role is not removed")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, excludedUsername, true)
		})

		It("should retain a stale PostgreSQL role when stale user deletion policy is Restrict", func() {
			resourceName := env.name("test-restrict-retain-role")
			generatedSecret := env.name("test-restrict-retain-role-secret")

			By("creating a PostgresAccess resource")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils2.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource")

			By("waiting for the managed role to exist")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)

			By("deleting the PostgresAccess resource")
			err = utils2.DeletePostgresAccess(resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

			By("verifying the managed role is retained by the default Restrict policy")
			utils2.WaitForResourceDeleted("postgresaccess", resourceName, env.namespace)
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)
			utils2.WaitForSecretDeleted(env.namespace, generatedSecret)
		})

		It("should drop owned objects when stale user deletion policy is Cascade", func() {
			managedUsername := env.name("test-cascade-cleanup")
			generatedSecret := env.name("test-cascade-cleanup-credentials")
			ownedTable := env.name("cascade-policy-owned-table")
			controllerName := env.name("postgres-cascade-policy")

			By("creating a singleton Controller with staleUserDeletionPolicy Cascade")
			err := createControllerResource(controllerName, namespace, `postgres:
  staleUserDeletionPolicy: Cascade`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create singleton Controller with Cascade policy")
			DeferCleanup(func() {
				deleteControllerResource(controllerName, namespace)
			})

			By("creating a PostgresAccess resource")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = utils2.CreateResourceFromSecretReference(
				managedUsername,
				env.namespace,
				generatedSecret,
				secretName,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "USAGE", "CREATE"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess with Cascade controller policy")

			By("waiting for the generated secret and managed role")
			managedPassword := utils2.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, true)

			By("creating an object owned by the managed user")
			managedConn := env.conn
			managedConn.Username = managedUsername
			managedConn.Password = managedPassword
			_, err = utils2.RunPostgresQuery(
				env.backendNamespace,
				managedConn,
				fmt.Sprintf(`CREATE TABLE public.%q (id SERIAL PRIMARY KEY, value TEXT);`, ownedTable),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create an owned object as the managed user")

			By("deleting the PostgresAccess resource")
			err = utils2.DeletePostgresAccess(managedUsername, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

			By("verifying the managed role and owned table are removed")
			utils2.WaitForResourceDeleted("postgresaccess", managedUsername, env.namespace)
			utils2.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, false)
			utils2.WaitForTableMissing(env.backendNamespace, env.conn, ownedTable)
		})

		It("should reject PostgresAccess manifests that still use spec.cleanupPolicy", func() {
			resourceName := env.name("test-cleanup-policy-schema-rejection")
			generatedSecret := env.name("test-cleanup-policy-schema-rejection-secret")

			By("creating the connection secret referenced by the invalid manifest")
			secretName, err := utils2.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			invalidManifest := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: PostgresAccess
metadata:
  name: %s
  namespace: %s
spec:
  generatedSecret: %s
  username: %s
  cleanupPolicy: Orphan
  connection:
    existingSecret: %s
  grants:
    - database: %s
      privileges:
        - CONNECT
`, resourceName, env.namespace, generatedSecret, resourceName, secretName, env.conn.Database)

			err = utils2.ApplyManifestServerDryRun(invalidManifest)
			Expect(err).To(HaveOccurred(), "PostgresAccess manifests using spec.cleanupPolicy should be rejected by the CRD schema")
		})
	})
})
