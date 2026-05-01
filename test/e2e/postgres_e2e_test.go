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

	e2eutils "github.com/delta10/access-operator/test/e2e/utils"
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

		err := e2eutils.ApplyManifest(invalidResource)
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
			err := e2eutils.DeployCNPGInstance(testNamespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to deploy PGSQL instance")

			By("waiting for CNPG to accept SQL connections")
			conn := e2eutils.GetCNPGConnectionDetailsFromSecret(testNamespace, "cnpg-postgres-app")
			e2eutils.WaitForAuthenticationSuccess(testNamespace, conn, conn.Username, conn.Password)

			resourceName := fmt.Sprintf("test-username-%s", uniqueSuffix())
			generatedSecret := fmt.Sprintf("test-postgres-credentials-%s", uniqueSuffix())

			By("creating a PostgresAccess resource referencing the connection secret")
			err = e2eutils.CreateResourceFromSecretReference(
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
			e2eutils.WaitForSecretField(testNamespace, generatedSecret, "username")

			By("verifying the database user was created")
			e2eutils.WaitForDatabaseUserState(testNamespace, conn, resourceName, true)
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
			err := e2eutils.CreatePostgresAccessWithDirectConnection(
				resourceName,
				env.namespace,
				generatedSecret,
				env.conn,
				[]string{"CONNECT", "SELECT"},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with connection details")

			By("waiting for the generated secret to be created")
			e2eutils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the database user was created")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)

			By("verifying the privileges were granted")
			e2eutils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})
		})

		It("should create a PostgresAccess resource with connectivity as a secret reference and create a database user accordingly", func() {
			resourceName := env.name("test-username")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a secret with the connection details")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			By("creating a PostgresAccess resource referencing the connection secret")
			err = e2eutils.CreateResourceFromSecretReference(
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
			e2eutils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the database user was created")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)
		})

		It("should create a database user when connection is provided via direct connection details but user and pass via secret reference", func() {
			resourceName := env.name("test-user-pass")
			generatedSecret := env.name("test-user-pass-secret")

			By("creating a secret with the username and password")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			By("creating a PostgresAccess resource referencing the username/password secret and providing connection details directly")
			err = e2eutils.CreatePostgresAccessWithConnectionSecretRef(
				resourceName,
				env.namespace,
				generatedSecret,
				env.conn,
				secretName,
				[]string{"CONNECT", "SELECT"},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with secret reference for username/password")

			By("waiting for the generated secret to be created")
			e2eutils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the database user was created")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)
		})

		It("should reconcile privileges when they're changed in the config", func() {
			resourceName := env.name("test-privileges-reconciliation")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource with certain privileges")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = e2eutils.CreateResourceFromSecretReference(
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
			e2eutils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT"})

			By("updating the PostgresAccess resource to include additional privileges")
			err = e2eutils.CreateResourceFromSecretReference(
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
			e2eutils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT", "INSERT"})
		})

		It("should reconcile the privileges of a PostgresAccess resource when they are manually revoked in the database", func() {
			resourceName := env.name("test-privileges-maintenance")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = e2eutils.CreateResourceFromSecretReference(
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
			e2eutils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

			By("revoking the SELECT privilege from the database user")
			_, err = e2eutils.RunPostgresQuery(
				env.backendNamespace,
				env.conn,
				fmt.Sprintf(`REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM "%s";`, resourceName),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to revoke SELECT privilege")

			err = e2eutils.TriggerReconciliation("postgresaccess", resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to trigger reconciliation after revoking privileges")

			By("verifying that the controller reconciles and restores the revoked privilege")
			e2eutils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})
		})

		It("should retain the database user and delete the generated secret when the PostgresAccess resource is deleted by default", func() {
			resourceName := env.name("test-deletion")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = e2eutils.CreateResourceFromSecretReference(
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
			e2eutils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

			By("deleting the PostgresAccess resource")
			err = e2eutils.DeletePostgresAccess(resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

			By("verifying finalization removed the PostgresAccess resource")
			e2eutils.WaitForResourceDeleted("postgresaccess", resourceName, env.namespace)

			By("verifying that the database user is retained by default policy")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)

			By("verifying that the generated secret is deleted")
			e2eutils.WaitForSecretDeleted(env.namespace, generatedSecret)
		})

		It("should reassign owned objects to the database owner when stale user deletion policy is Orphan", Serial, func() {
			managedUsername := env.name("test-orphan-cleanup")
			generatedSecret := env.name("test-orphan-cleanup-credentials")
			ownedTable := env.name("orphan-policy-owned-table")
			By("creating a settings ConfigMap with staleUserDeletionPolicy Orphan")
			err := createControllerSettingsConfigMap(namespace, `postgres:
  staleUserDeletionPolicy: Orphan`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create settings ConfigMap with Orphan policy")
			DeferCleanup(func() {
				deleteControllerSettingsConfigMap(namespace)
			})

			By("creating a PostgresAccess resource")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = e2eutils.CreateResourceFromSecretReference(
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
			managedPassword := e2eutils.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")

			By("waiting for the managed user to be created")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, true)

			By("creating an object owned by the managed user")
			managedConn := env.conn
			managedConn.Username = managedUsername
			managedConn.Password = managedPassword
			_, err = e2eutils.RunPostgresQuery(
				env.backendNamespace,
				managedConn,
				fmt.Sprintf(`CREATE TABLE public.%q (id SERIAL PRIMARY KEY, value TEXT);`, ownedTable),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create an owned object as the managed user")

			By("verifying the object is initially owned by the managed user")
			e2eutils.WaitForTableOwner(env.backendNamespace, env.conn, ownedTable, managedUsername)

			By("deleting the PostgresAccess resource")
			err = e2eutils.DeletePostgresAccess(managedUsername, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

			By("verifying that the managed role is deleted")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, false)

			By("verifying ownership is reassigned to the current database owner")
			e2eutils.WaitForTableOwner(env.backendNamespace, env.conn, ownedTable, env.conn.Username)
		})

		It("should update the database user's password when the PostgresAccess resource is updated with a new password", func() {
			resourceName := env.name("test-password-update")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = e2eutils.CreateResourceFromSecretReference(
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
			e2eutils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

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

			err = e2eutils.ApplyManifest(updatedSecretYAML)
			Expect(err).NotTo(HaveOccurred(), "Failed to update generated secret with new password")

			By("verifying that the database user's password is updated and the user can authenticate with the new password")
			e2eutils.WaitForAuthenticationSuccess(env.backendNamespace, env.conn, resourceName, newPassword)
		})

		It("should update the database user's password the secret's password is rolled via deletion", func() {
			resourceName := env.name("test-password-rotation")
			generatedSecret := env.name("test-postgres-credentials-secret-ref")

			By("creating a PostgresAccess resource")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = e2eutils.CreateResourceFromSecretReference(
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
			e2eutils.WaitForPrivilegesGranted(env.backendNamespace, env.conn, resourceName, []string{"CONNECT", "SELECT"})

			By("deleting the secret to trigger password rotation")
			cmd := exec.Command("kubectl", "delete", "secret", generatedSecret, "-n", env.namespace)
			_, err = e2eutils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete generated secret")

			By("verifying that the database user's password is updated and the user can authenticate with the new password")
			newPassword := e2eutils.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")
			e2eutils.WaitForAuthenticationSuccess(env.backendNamespace, env.conn, resourceName, newPassword)
		})
	})

	Context("Settings ConfigMap policy", Serial, func() {
		var env postgresSpecEnv

		BeforeEach(func() {
			clearAllControllerSettingsConfigMaps()
			env = newPostgresSpecEnv()
		})

		AfterEach(func() {
			env.cleanup()
			clearAllControllerSettingsConfigMaps()
		})

		It("should deny cross-namespace existingSecret when no settings ConfigMap exists", func() {
			resourceName := env.name("test-cross-namespace-no-controller")
			generatedSecret := env.name("test-cross-namespace-no-controller-secret")
			connectionSecretNamespace := createTestNamespace("postgres-shared-no-controller")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})

			By("creating the connection secret in another namespace")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = e2eutils.CreateResourceFromSecretReferenceWithNamespace(
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
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, false)

			By("removing the denied PostgresAccess resource without running its unreachable finalizer")
			forceDeleteAccessResource("postgresaccess", namespacedName{name: resourceName, namespace: env.namespace})
		})

		It("should deny cross-namespace existingSecret when settings ConfigMap setting is false", func() {
			resourceName := env.name("test-cross-namespace-controller-false")
			generatedSecret := env.name("test-cross-namespace-controller-false-secret")
			connectionSecretNamespace := createTestNamespace("postgres-shared-controller-false")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})

			By("creating settings ConfigMap with existingSecretNamespace=false")
			err := createControllerSettingsConfigMap(namespace, `existingSecretNamespace: false`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create settings ConfigMap with false policy")
			DeferCleanup(func() {
				deleteControllerSettingsConfigMap(namespace)
			})

			By("creating the connection secret in another namespace")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = e2eutils.CreateResourceFromSecretReferenceWithNamespace(
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

			By("verifying reconcile is denied because settings ConfigMap policy is false")
			waitForReadyCondition("postgresaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				messageContains: "cross-namespace connection secret references are disabled",
			})

			By("verifying the requested database user was not created")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, false)

			By("removing the denied PostgresAccess resource without running its unreachable finalizer")
			forceDeleteAccessResource("postgresaccess", namespacedName{name: resourceName, namespace: env.namespace})
		})

		It("should create a PostgresAccess resource using an existing connection secret from another namespace", func() {
			resourceName := env.name("test-username-cross-namespace")
			generatedSecret := env.name("test-postgres-credentials-cross-namespace")
			connectionSecretNamespace := createTestNamespace("postgres-shared")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})

			By("enabling cross-namespace references through operator settings ConfigMap")
			err := createControllerSettingsConfigMap(namespace, `existingSecretNamespace: true`)
			Expect(err).NotTo(HaveOccurred(), "Failed to enable cross-namespace references via settings ConfigMap")
			DeferCleanup(func() {
				deleteControllerSettingsConfigMap(namespace)
			})

			By("creating the connection secret in the shared namespace")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess resource in the workload namespace that references the shared secret")
			err = e2eutils.CreateResourceFromSecretReferenceWithNamespace(
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
			e2eutils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the database user was created")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)
		})

		It("should deny cross-namespace existingSecret when settings ConfigMap is outside the operator namespace", func() {
			resourceName := env.name("test-cross-namespace-wrong-controller-namespace")
			generatedSecret := env.name("test-cross-namespace-wrong-controller-namespace-secret")
			connectionSecretNamespace := createTestNamespace("postgres-shared-wrong-controller-namespace")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})

			By("creating settings ConfigMap in workload namespace instead of operator namespace")
			err := createControllerSettingsConfigMap(env.namespace, `existingSecretNamespace: true`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create settings ConfigMap outside the operator namespace")
			DeferCleanup(func() {
				deleteControllerSettingsConfigMap(env.namespace)
			})

			By("creating the connection secret in another namespace")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a PostgresAccess that references the shared secret namespace")
			err = e2eutils.CreateResourceFromSecretReferenceWithNamespace(
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

			By("verifying reconcile is denied because settings ConfigMap outside operator namespace is ignored")
			waitForReadyCondition("postgresaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				status:          "False",
				reason:          "DatabaseSyncFailed",
				messageContains: "cross-namespace connection secret references are disabled",
			})

			By("verifying the requested database user was not created")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, false)

			By("removing the denied PostgresAccess resource without running its unreachable finalizer")
			forceDeleteAccessResource("postgresaccess", namespacedName{name: resourceName, namespace: env.namespace})
		})

		It("should preserve excluded PostgreSQL users from settings ConfigMap", func() {
			excludedUsername := env.name("excluded-keeper")
			managedUsername := env.name("test-managed-user")
			generatedSecret := env.name("test-excluded-user-secret")

			By("creating settings ConfigMap with excluded PostgreSQL users")
			err := createControllerSettingsConfigMap(namespace, fmt.Sprintf(`postgres:
  excludedUsers:
    - %s`, excludedUsername))
			Expect(err).NotTo(HaveOccurred(), "Failed to create settings ConfigMap with excluded users")
			DeferCleanup(func() {
				deleteControllerSettingsConfigMap(namespace)
			})

			By("creating an unmanaged PostgreSQL role that should be preserved")
			_, err = e2eutils.RunPostgresQuery(
				env.backendNamespace,
				env.conn,
				fmt.Sprintf(`CREATE ROLE "%s" WITH LOGIN PASSWORD 'keep-me';`, excludedUsername),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create excluded PostgreSQL role")

			By("creating a PostgresAccess resource to trigger reconciliation")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = e2eutils.CreateResourceFromSecretReference(
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

			By("waiting for the generated secret to be created")
			e2eutils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the managed role is created")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, true)

			By("verifying the excluded unmanaged role is not removed")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, excludedUsername, true)
		})

		It("should retain a stale PostgreSQL role when stale user deletion policy is Restrict", func() {
			resourceName := env.name("test-restrict-retain-role")
			generatedSecret := env.name("test-restrict-retain-role-secret")

			By("creating a PostgresAccess resource")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = e2eutils.CreateResourceFromSecretReference(
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

			By("waiting for the generated secret to be created")
			e2eutils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("waiting for the managed role to exist")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)

			By("deleting the PostgresAccess resource")
			err = e2eutils.DeletePostgresAccess(resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

			By("verifying the managed role is retained by the default Restrict policy")
			e2eutils.WaitForResourceDeleted("postgresaccess", resourceName, env.namespace)
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)
			e2eutils.WaitForSecretDeleted(env.namespace, generatedSecret)
		})

		It("should drop owned objects when stale user deletion policy is Cascade", func() {
			managedUsername := env.name("test-cascade-cleanup")
			generatedSecret := env.name("test-cascade-cleanup-credentials")
			ownedTable := env.name("cascade-policy-owned-table")
			By("creating a settings ConfigMap with staleUserDeletionPolicy Cascade")
			err := createControllerSettingsConfigMap(namespace, `postgres:
  staleUserDeletionPolicy: Cascade`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create settings ConfigMap with Cascade policy")
			DeferCleanup(func() {
				deleteControllerSettingsConfigMap(namespace)
			})

			By("creating a PostgresAccess resource")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = e2eutils.CreateResourceFromSecretReference(
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
			managedPassword := e2eutils.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, true)

			By("creating an object owned by the managed user")
			managedConn := env.conn
			managedConn.Username = managedUsername
			managedConn.Password = managedPassword
			_, err = e2eutils.RunPostgresQuery(
				env.backendNamespace,
				managedConn,
				fmt.Sprintf(`CREATE TABLE public.%q (id SERIAL PRIMARY KEY, value TEXT);`, ownedTable),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create an owned object as the managed user")

			By("deleting the PostgresAccess resource")
			err = e2eutils.DeletePostgresAccess(managedUsername, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

			By("verifying the managed role and owned table are removed")
			e2eutils.WaitForResourceDeleted("postgresaccess", managedUsername, env.namespace)
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, managedUsername, false)
			e2eutils.WaitForTableMissing(env.backendNamespace, env.conn, ownedTable)
		})

		It("should delete the managed role during finalization when stale user deletion policy is Retain", func() {
			resourceName := env.name("test-retain-finalizer-delete")
			generatedSecret := env.name("test-retain-finalizer-delete-secret")
			By("creating a settings ConfigMap with staleUserDeletionPolicy Retain")
			err := createControllerSettingsConfigMap(namespace, `postgres:
  staleUserDeletionPolicy: Retain`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create settings ConfigMap with Retain policy")
			DeferCleanup(func() {
				deleteControllerSettingsConfigMap(namespace)
			})

			By("creating a PostgresAccess resource")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret")

			err = e2eutils.CreateResourceFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecret,
				secretName,
				accessv1.GrantSpec{
					Database:   env.conn.Database,
					Privileges: []string{"CONNECT", "SELECT"},
				},
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PostgresAccess resource with Retain controller policy")

			By("waiting for the generated secret to be created")
			e2eutils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("waiting for the managed role to exist")
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, true)

			By("deleting the PostgresAccess resource")
			err = e2eutils.DeletePostgresAccess(resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete PostgresAccess resource")

			By("verifying the managed role is deleted during finalization")
			e2eutils.WaitForResourceDeleted("postgresaccess", resourceName, env.namespace)
			e2eutils.WaitForDatabaseUserState(env.backendNamespace, env.conn, resourceName, false)
			e2eutils.WaitForSecretDeleted(env.namespace, generatedSecret)
		})

		It("should reject PostgresAccess manifests that still use spec.cleanupPolicy", func() {
			resourceName := env.name("test-cleanup-policy-schema-rejection")
			generatedSecret := env.name("test-cleanup-policy-schema-rejection-secret")

			By("creating the connection secret referenced by the invalid manifest")
			secretName, err := e2eutils.CreateConnectionDetailsViaSecret(env.namespace, env.conn)
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

			err = e2eutils.ApplyManifestServerDryRun(invalidManifest)
			Expect(err).To(HaveOccurred(), "PostgresAccess manifests using spec.cleanupPolicy should be rejected by the CRD schema")
		})
	})
})
