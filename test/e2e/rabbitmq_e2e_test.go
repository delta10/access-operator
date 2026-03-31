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
    "fmt"
    "os/exec"
    "strings"

    utils2 "github.com/delta10/access-operator/test/e2e/utils"
    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"

    accessv1 "github.com/delta10/access-operator/api/v1"
)

var _ = Describe("RabbitMQ", func() {
	Context("RabbitMQ", func() {
		var env rabbitMQSpecEnv

		BeforeEach(func() {
			env = newRabbitMQSpecEnv()
		})

		AfterEach(func() {
			env.cleanup()
		})

		It("should log reconcile errors and set Ready=False when connection details are invalid", func() {
			resourceName := env.name("invalid-rabbitmq-connection")
			generatedSecretName := env.name("invalid-rabbitmq-connection-secret")
			vhost := env.vhost("app")

			By("creating a RabbitMQAccess resource with invalid connection details")
			invalidResource := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: RabbitMQAccess
metadata:
  name: %s
  namespace: %s
spec:
  generatedSecret: %s
  username: %s
  connection: {}
  permissions:
    - vhost: %s
      configure: ".*"
      write: ".*"
      read: ".*"
`, resourceName, env.namespace, generatedSecretName, resourceName, vhost)

			err := utils2.ApplyManifest(invalidResource)
			Expect(err).NotTo(HaveOccurred(), "Failed to create invalid RabbitMQAccess resource")

			By("verifying the RabbitMQAccess status reports the reconcile failure")
			waitForReadyCondition("rabbitmqaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				status: "False",
				reason: "ConnectionError",
			})

			By("verifying the controller logs the expected reconcile error")
			waitForControllerLogsContain(resourceName, "no valid connection details provided")
		})

		It("should create a RabbitMQAccess resource and create a RabbitMQ user with the specified permissions via direct connection details", func() {
			resourceName := env.name("test-rabbitmq-access")
			generatedSecret := env.name("test-rabbitmq-credentials")
			vhost := env.vhost("app")
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: vhost, Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a RabbitMQAccess resource")
			err := utils2.CreateRabbitMQAccessWithDirectConnection(resourceName, env.namespace, generatedSecret, env.conn, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource with connection details")

			By("waiting for the generated secret to be created")
			utils2.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the RabbitMQ user and vhost were created")
			utils2.WaitForRabbitMQUserState(env.backendNamespace, resourceName, true)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, vhost, true)

			By("verifying the permissions were granted")
			utils2.WaitForRabbitMQPermissions(env.backendNamespace, resourceName, permissions)

			By("verifying the generated credentials can authenticate")
			password := utils2.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")
			utils2.WaitForRabbitMQAuthenticationSuccess(env.backendNamespace, resourceName, password)
		})

		It("should create a RabbitMQAccess resource with direct host/port and secret-referenced credentials", func() {
			resourceName := env.name("test-rabbitmq-secret-ref")
			generatedSecret := env.name("test-rabbitmq-secret-ref-credentials")
			vhost := env.vhost("app")
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: vhost, Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a secret with the connection details")
			secretName, err := utils2.CreateRabbitMQConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQ connection secret")

			By("creating a RabbitMQAccess resource referencing the username/password secret and providing host/port directly")
			err = utils2.CreateRabbitMQAccessWithConnectionSecretRef(resourceName, env.namespace, generatedSecret, env.conn, secretName, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource with secret references")

			By("waiting for the generated secret to be created")
			utils2.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the RabbitMQ user and permissions were created")
			utils2.WaitForRabbitMQUserState(env.backendNamespace, resourceName, true)
			utils2.WaitForRabbitMQPermissions(env.backendNamespace, resourceName, permissions)
		})

		It("should create a RabbitMQAccess resource using an existing connection secret in the same namespace", func() {
			resourceName := env.name("test-rabbitmq-existing-secret")
			generatedSecret := env.name("test-rabbitmq-existing-secret-credentials")
			vhost := env.vhost("app")
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: vhost, Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a secret with the connection details")
			secretName, err := utils2.CreateRabbitMQConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQ connection secret")

			By("creating a RabbitMQAccess resource referencing the connection secret")
			err = utils2.CreateRabbitMQAccessFromSecretReference(resourceName, env.namespace, generatedSecret, secretName, nil, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource with existingSecret")

			By("waiting for the generated secret to be created")
			utils2.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the RabbitMQ user and permissions were created")
			utils2.WaitForRabbitMQUserState(env.backendNamespace, resourceName, true)
			utils2.WaitForRabbitMQPermissions(env.backendNamespace, resourceName, permissions)
		})

		It("should delete the RabbitMQ user and generated secret when the RabbitMQAccess resource is deleted", func() {
			resourceName := env.name("test-rabbitmq-deletion")
			generatedSecret := env.name("test-rabbitmq-deletion-secret")
			vhost := env.vhost("app-delete")
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: vhost, Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a RabbitMQAccess resource")
			err := utils2.CreateRabbitMQAccessWithDirectConnection(resourceName, env.namespace, generatedSecret, env.conn, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource")

			By("waiting for the generated secret, user, and permissions to exist")
			utils2.WaitForSecretField(env.namespace, generatedSecret, "username")
			utils2.WaitForRabbitMQUserState(env.backendNamespace, resourceName, true)
			utils2.WaitForRabbitMQPermissions(env.backendNamespace, resourceName, permissions)

			By("deleting the RabbitMQAccess resource")
			err = utils2.DeleteRabbitMQAccess(resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RabbitMQAccess resource")

			By("verifying finalization removed the RabbitMQAccess, user, and generated secret")
			utils2.WaitForResourceDeleted("rabbitmqaccess", resourceName, env.namespace)
			utils2.WaitForRabbitMQUserState(env.backendNamespace, resourceName, false)
			utils2.WaitForSecretDeleted(env.namespace, generatedSecret)
		})

		It("should reconcile permissions when they're changed in the config", func() {
			resourceName := env.name("test-rabbitmq-permissions-reconciliation")
			generatedSecret := env.name("test-rabbitmq-permissions-reconciliation-secret")
			vhost := env.vhost("app")
			initialPermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: vhost, Configure: "^$", Write: "^$", Read: "^$"},
			}
			updatedPermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: vhost, Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a RabbitMQAccess resource with certain permissions")
			err := utils2.CreateRabbitMQAccessWithDirectConnection(resourceName, env.namespace, generatedSecret, env.conn, initialPermissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource")

			By("waiting for the initial permissions to be granted")
			utils2.WaitForRabbitMQPermissions(env.backendNamespace, resourceName, initialPermissions)

			By("updating the RabbitMQAccess resource to include new permissions")
			err = utils2.CreateRabbitMQAccessWithDirectConnection(resourceName, env.namespace, generatedSecret, env.conn, updatedPermissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to update RabbitMQAccess resource")

			By("verifying that the new permissions are granted")
			utils2.WaitForRabbitMQPermissions(env.backendNamespace, resourceName, updatedPermissions)
		})

		It("should reconcile the permissions of a RabbitMQAccess resource when they are manually revoked", func() {
			resourceName := env.name("test-rabbitmq-permissions-maintenance")
			generatedSecret := env.name("test-rabbitmq-permissions-maintenance-secret")
			vhost := env.vhost("app")
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: vhost, Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a RabbitMQAccess resource")
			err := utils2.CreateRabbitMQAccessWithDirectConnection(resourceName, env.namespace, generatedSecret, env.conn, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource")

			By("waiting for the permissions to be granted")
			utils2.WaitForRabbitMQPermissions(env.backendNamespace, resourceName, permissions)

			By("revoking the permissions from the RabbitMQ user")
			_, err = utils2.RunRabbitMQctl(env.backendNamespace, "clear_permissions", "-p", vhost, resourceName)
			Expect(err).NotTo(HaveOccurred(), "Failed to clear RabbitMQ permissions")

			err = utils2.TriggerReconciliation("rabbitmqaccess", resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to trigger reconciliation after clearing permissions")

			By("verifying that the controller reconciles and restores the permissions")
			utils2.WaitForRabbitMQPermissions(env.backendNamespace, resourceName, permissions)
		})

		It("should update the RabbitMQ user's password when the secret's password is rolled via deletion", func() {
			resourceName := env.name("test-rabbitmq-password-rotation")
			generatedSecret := env.name("test-rabbitmq-password-rotation-secret")
			vhost := env.vhost("app")
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: vhost, Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a RabbitMQAccess resource")
			err := utils2.CreateRabbitMQAccessWithDirectConnection(resourceName, env.namespace, generatedSecret, env.conn, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource")

			By("waiting for the generated secret and initial authentication")
			oldPassword := utils2.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")
			utils2.WaitForRabbitMQAuthenticationSuccess(env.backendNamespace, resourceName, oldPassword)

			By("deleting the generated secret to trigger password rotation")
			cmd := exec.Command("kubectl", "delete", "secret", generatedSecret, "-n", env.namespace)
			_, err = utils2.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete generated secret")

			By("verifying that the RabbitMQ user's password is rotated and the new password authenticates")
			newPassword := utils2.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")
			Expect(newPassword).NotTo(Equal(oldPassword))
			utils2.WaitForRabbitMQAuthenticationSuccess(env.backendNamespace, resourceName, newPassword)
		})
	})

	Context("Controller policy", Serial, func() {
		var env rabbitMQSpecEnv

		BeforeEach(func() {
			clearAllControllers()
			env = newRabbitMQSpecEnv()
		})

		AfterEach(func() {
			env.cleanup()
			clearAllControllers()
		})

		It("should delete stale RabbitMQ vhosts when singleton Controller policy enables deletion", func() {
			controllerName := env.name("rabbitmq-vhost-cleanup-delete")
			keeperName := env.name("test-rabbitmq-vhost-keeper")
			staleName := env.name("test-rabbitmq-vhost-stale")
			keeperVhost := env.vhost("keeper")
			staleVhost := env.vhost("orphan")
			deletePolicy := accessv1.StaleVhostDeletionPolicyDelete
			keeperPermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: keeperVhost, Configure: ".*", Write: ".*", Read: ".*"},
			}
			stalePermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: staleVhost, Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("enabling stale RabbitMQ vhost deletion through singleton Controller settings")
			err := createRabbitMQController(controllerName, &deletePolicy, nil)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQ controller settings")
			DeferCleanup(func() {
				deleteControllerResource(controllerName, namespace)
			})

			By("creating a keeper RabbitMQAccess resource")
			err = utils2.CreateRabbitMQAccessWithDirectConnection(keeperName, env.namespace, env.name("keeper-secret"), env.conn, keeperPermissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create keeper RabbitMQAccess resource")

			By("creating a second RabbitMQAccess resource that owns an orphanable vhost")
			err = utils2.CreateRabbitMQAccessWithDirectConnection(staleName, env.namespace, env.name("stale-secret"), env.conn, stalePermissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create stale RabbitMQAccess resource")

			By("waiting for both RabbitMQ users and vhosts to exist")
			utils2.WaitForRabbitMQUserState(env.backendNamespace, keeperName, true)
			utils2.WaitForRabbitMQUserState(env.backendNamespace, staleName, true)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, keeperVhost, true)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, staleVhost, true)

			By("deleting the stale RabbitMQAccess resource")
			err = utils2.DeleteRabbitMQAccess(staleName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete stale RabbitMQAccess resource")

			By("verifying finalization deletes the stale user and its vhost while the keeper remains")
			utils2.WaitForResourceDeleted("rabbitmqaccess", staleName, env.namespace)
			utils2.WaitForRabbitMQUserState(env.backendNamespace, staleName, false)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, staleVhost, false)
			utils2.WaitForRabbitMQUserState(env.backendNamespace, keeperName, true)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, keeperVhost, true)
		})

		It("should retain orphaned RabbitMQ vhosts when stale vhost deletion is not enabled", func() {
			keeperName := env.name("test-rabbitmq-vhost-retain-keeper")
			staleName := env.name("test-rabbitmq-vhost-retain-stale")
			keeperVhost := env.vhost("keeper-retain")
			staleVhost := env.vhost("retained")
			keeperPermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: keeperVhost, Configure: ".*", Write: ".*", Read: ".*"},
			}
			stalePermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: staleVhost, Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a keeper RabbitMQAccess resource")
			err := utils2.CreateRabbitMQAccessWithDirectConnection(
				keeperName,
				env.namespace,
				env.name("keeper-secret"),
				env.conn,
				keeperPermissions,
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create keeper RabbitMQAccess resource")

			By("creating a second RabbitMQAccess resource whose vhost should be retained")
			err = utils2.CreateRabbitMQAccessWithDirectConnection(
				staleName,
				env.namespace,
				env.name("stale-secret"),
				env.conn,
				stalePermissions,
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create stale RabbitMQAccess resource")

			By("waiting for both RabbitMQ users and vhosts to exist")
			utils2.WaitForRabbitMQUserState(env.backendNamespace, keeperName, true)
			utils2.WaitForRabbitMQUserState(env.backendNamespace, staleName, true)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, keeperVhost, true)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, staleVhost, true)

			By("deleting the stale RabbitMQAccess resource")
			err = utils2.DeleteRabbitMQAccess(staleName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete stale RabbitMQAccess resource")

			By("verifying finalization deletes the stale user but retains its vhost by policy")
			utils2.WaitForResourceDeleted("rabbitmqaccess", staleName, env.namespace)
			utils2.WaitForRabbitMQUserState(env.backendNamespace, staleName, false)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, staleVhost, true)
			utils2.WaitForRabbitMQUserState(env.backendNamespace, keeperName, true)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, keeperVhost, true)
		})

		It("should preserve excluded RabbitMQ vhosts when stale vhost deletion is enabled", func() {
			controllerName := env.name("rabbitmq-vhost-cleanup-excluded")
			keeperName := env.name("test-rabbitmq-vhost-excluded-keeper")
			staleName := env.name("test-rabbitmq-vhost-excluded-stale")
			keeperVhost := env.vhost("keeper-excluded")
			protectedVhost := env.vhost("protected")
			deletePolicy := accessv1.StaleVhostDeletionPolicyDelete
			keeperPermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: keeperVhost, Configure: ".*", Write: ".*", Read: ".*"},
			}
			stalePermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: protectedVhost, Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("enabling stale RabbitMQ vhost deletion while excluding the protected vhost")
			err := createRabbitMQController(controllerName, &deletePolicy, []string{protectedVhost})
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQ controller settings")
			DeferCleanup(func() {
				deleteControllerResource(controllerName, namespace)
			})

			By("creating a keeper RabbitMQAccess resource")
			err = utils2.CreateRabbitMQAccessWithDirectConnection(
				keeperName,
				env.namespace,
				env.name("keeper-secret"),
				env.conn,
				keeperPermissions,
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create keeper RabbitMQAccess resource")

			By("creating a second RabbitMQAccess resource that uses the excluded vhost")
			err = utils2.CreateRabbitMQAccessWithDirectConnection(
				staleName,
				env.namespace,
				env.name("stale-secret"),
				env.conn,
				stalePermissions,
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create stale RabbitMQAccess resource")

			By("waiting for both RabbitMQ users and vhosts to exist")
			utils2.WaitForRabbitMQUserState(env.backendNamespace, keeperName, true)
			utils2.WaitForRabbitMQUserState(env.backendNamespace, staleName, true)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, keeperVhost, true)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, protectedVhost, true)

			By("deleting the stale RabbitMQAccess resource")
			err = utils2.DeleteRabbitMQAccess(staleName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete stale RabbitMQAccess resource")

			By("verifying finalization deletes the stale user but retains the excluded vhost")
			utils2.WaitForResourceDeleted("rabbitmqaccess", staleName, env.namespace)
			utils2.WaitForRabbitMQUserState(env.backendNamespace, staleName, false)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, protectedVhost, true)
			utils2.WaitForRabbitMQUserState(env.backendNamespace, keeperName, true)
			utils2.WaitForRabbitMQVhostState(env.backendNamespace, keeperVhost, true)
		})

		It("should deny cross-namespace existingSecret when no Controller resource exists", func() {
			resourceName := env.name("test-rabbitmq-cross-namespace-no-controller")
			generatedSecretName := env.name("test-rabbitmq-cross-namespace-no-controller-secret")
			connectionSecretNamespace := createTestNamespace("rabbitmq-shared-no-controller")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: env.vhost("app"), Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating the connection secret in another namespace")
			secretName, err := utils2.CreateRabbitMQConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RabbitMQAccess that references the shared secret namespace")
			err = utils2.CreateRabbitMQAccessFromSecretReference(resourceName, env.namespace, generatedSecretName, secretName, &connectionSecretNamespace, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace RabbitMQAccess")

			By("verifying reconcile is denied with cross-namespace policy disabled")
			waitForReadyCondition("rabbitmqaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				status:          "False",
				reason:          "ConnectionError",
				messageContains: "cross-namespace connection secret references are disabled",
			})

			By("verifying the requested RabbitMQ user was not created")
			utils2.WaitForRabbitMQUserState(env.backendNamespace, resourceName, false)
		})

		It("should deny cross-namespace existingSecret when singleton Controller setting is false", func() {
			resourceName := env.name("test-rabbitmq-cross-namespace-controller-false")
			generatedSecretName := env.name("test-rabbitmq-cross-namespace-controller-false-secret")
			controllerName := env.name("rabbitmq-cluster-settings-false")
			connectionSecretNamespace := createTestNamespace("rabbitmq-shared-controller-false")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: env.vhost("app"), Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a singleton Controller with existingSecretNamespace=false")
			err := createControllerResource(controllerName, namespace, `existingSecretNamespace: false`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create singleton Controller with false policy")
			DeferCleanup(func() {
				deleteControllerResource(controllerName, namespace)
			})

			By("creating the connection secret in another namespace")
			secretName, err := utils2.CreateRabbitMQConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RabbitMQAccess that references the shared secret namespace")
			err = utils2.CreateRabbitMQAccessFromSecretReference(resourceName, env.namespace, generatedSecretName, secretName, &connectionSecretNamespace, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace RabbitMQAccess")

			By("verifying reconcile is denied because singleton Controller policy is false")
			waitForReadyCondition("rabbitmqaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				messageContains: "cross-namespace connection secret references are disabled",
			})

			By("verifying the requested RabbitMQ user was not created")
			utils2.WaitForRabbitMQUserState(env.backendNamespace, resourceName, false)
		})

		It("should create a RabbitMQAccess resource using an existing connection secret from another namespace", func() {
			resourceName := env.name("test-rabbitmq-cross-namespace")
			generatedSecretName := env.name("test-rabbitmq-cross-namespace-credentials")
			controllerName := env.name("rabbitmq-cluster-settings")
			connectionSecretNamespace := createTestNamespace("rabbitmq-shared")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: env.vhost("app"), Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("enabling cross-namespace references through the singleton Controller resource")
			err := createControllerResource(controllerName, namespace, `existingSecretNamespace: true`)
			Expect(err).NotTo(HaveOccurred(), "Failed to enable cross-namespace references via Controller CR")
			DeferCleanup(func() {
				deleteControllerResource(controllerName, namespace)
			})

			By("creating the connection secret in the shared namespace")
			secretName, err := utils2.CreateRabbitMQConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RabbitMQAccess resource in the workload namespace that references the shared secret")
			err = utils2.CreateRabbitMQAccessFromSecretReference(
				resourceName,
				env.namespace,
				generatedSecretName,
				secretName,
				&connectionSecretNamespace,
				permissions,
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource with cross-namespace secret reference")

			By("waiting for the generated secret to be created")
			utils2.WaitForSecretField(env.namespace, generatedSecretName, "username")

			By("verifying the RabbitMQ user and permissions were created")
			utils2.WaitForRabbitMQUserState(env.backendNamespace, resourceName, true)
			utils2.WaitForRabbitMQPermissions(env.backendNamespace, resourceName, permissions)
		})

		It("should deny cross-namespace existingSecret when the singleton Controller is outside the operator namespace", func() {
			resourceName := env.name("test-rabbitmq-wrong-controller-namespace")
			generatedSecretName := env.name("test-rabbitmq-wrong-controller-namespace-secret")
			controllerName := env.name("rabbitmq-cluster-settings-wrong-namespace")
			connectionSecretNamespace := createTestNamespace("rabbitmq-shared-wrong-controller-namespace")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: env.vhost("app"), Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating the singleton Controller in a workload namespace instead of the operator namespace")
			err := createControllerResource(controllerName, env.namespace, `existingSecretNamespace: true`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create singleton Controller outside the operator namespace")
			DeferCleanup(func() {
				deleteControllerResource(controllerName, env.namespace)
			})

			By("creating the connection secret in another namespace")
			secretName, err := utils2.CreateRabbitMQConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RabbitMQAccess that references the shared secret namespace")
			err = utils2.CreateRabbitMQAccessFromSecretReference(resourceName, env.namespace, generatedSecretName, secretName, &connectionSecretNamespace, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace RabbitMQAccess")

			By("verifying reconcile is denied because the Controller is not in the operator namespace")
			waitForReadyCondition("rabbitmqaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				status:          "False",
				reason:          "ConnectionError",
				messageContains: `must be created in the operator namespace "access-operator-system"`,
			})

			By("verifying the misplaced Controller is marked not ready")
			waitForReadyCondition("controller", namespacedName{name: controllerName, namespace: env.namespace}, readyConditionExpectation{
				status:          "False",
				reason:          "InvalidControllerNamespace",
				messageContains: `must be created in the operator namespace "access-operator-system"`,
			})

			By("verifying the requested RabbitMQ user was not created")
			utils2.WaitForRabbitMQUserState(env.backendNamespace, resourceName, false)
		})

		It("should fail when multiple Controller resources exist and emit warning events", func() {
			resourceName := env.name("test-rabbitmq-multiple-controllers")
			generatedSecretName := env.name("test-rabbitmq-multiple-controllers-secret")
			controllerAName := env.name("rabbitmq-cluster-settings-a")
			controllerBName := env.name("rabbitmq-cluster-settings-b")
			connectionSecretNamespace := createTestNamespace("rabbitmq-shared-multiple-controller")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: env.vhost("app"), Configure: ".*", Write: ".*", Read: ".*"},
			}

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
			secretName, err := utils2.CreateRabbitMQConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RabbitMQAccess that references the shared secret namespace")
			err = utils2.CreateRabbitMQAccessFromSecretReference(resourceName, env.namespace, generatedSecretName, secretName, &connectionSecretNamespace, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace RabbitMQAccess")

			By("verifying RabbitMQAccess fails with multiple-controller error")
			waitForReadyCondition("rabbitmqaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				reason:          "ConnectionError",
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
	})
})

func createRabbitMQController(
	name string,
	policy *accessv1.StaleVhostDeletionPolicy,
	excludedVhosts []string,
) error {
	var settings strings.Builder

	if policy != nil || len(excludedVhosts) > 0 {
		settings.WriteString("rabbitmq:\n")
		if len(excludedVhosts) > 0 {
			settings.WriteString("  excludedVhosts:\n")
			for _, vhost := range excludedVhosts {
				settings.WriteString(fmt.Sprintf("    - %s\n", vhost))
			}
		}
		if policy != nil {
			settings.WriteString(fmt.Sprintf("  staleVhostDeletionPolicy: %s\n", *policy))
		}
	}

	return createControllerResource(name, namespace, settings.String())
}
