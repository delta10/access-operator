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
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/delta10/access-operator/api/v1"
	"github.com/delta10/access-operator/test/utils"
)

var _ = Describe("RabbitMQ", Ordered, func() {
	Context("RabbitMQ", func() {
		BeforeEach(func() {
			testNamespace := fmt.Sprintf("rabbitmq-access-test-%d", time.Now().UnixNano()%1_000_000)
			Expect(os.Setenv("RABBITMQ_TEST_NAMESPACE", testNamespace)).To(Succeed())

			By("creating a fresh test namespace")
			cmd := exec.Command("kubectl", "create", "ns", testNamespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create test namespace")

			_, conn := utils.GetRabbitMQVariables()

			By("ensuring singleton Controller resources are cleaned up before each RabbitMQ test")
			cmd = exec.Command("kubectl", "delete", "controller", "--all", "-n", namespace, "--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)

			By("deploying RabbitMQ in the test namespace")
			err = utils.DeployRabbitMQInstance(testNamespace, conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to deploy RabbitMQ instance")

			By("waiting for RabbitMQ to become ready")
			utils.WaitForRabbitMQReady(testNamespace)

			Expect(conn.Host).NotTo(BeEmpty(), "RabbitMQ host should be configured")
		})

		AfterEach(func() {
			testNamespace, _ := utils.GetRabbitMQVariables()

			By("cleaning up any remaining RabbitMQAccess resources")
			cmd := exec.Command("kubectl", "delete", "rabbitmqaccess", "--all", "-n", testNamespace, "--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)

			By("cleaning up the test namespace")
			cmd = exec.Command("kubectl", "delete", "ns", testNamespace, "--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)

			Expect(os.Unsetenv("RABBITMQ_TEST_NAMESPACE")).To(Succeed())
		})

		It("should log reconcile errors and set Ready=False when connection details are invalid", func() {
			testNamespace, _ := utils.GetRabbitMQVariables()
			resourceName := "invalid-rabbitmq-connection"
			generatedSecretName := "invalid-rabbitmq-connection-secret"

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
    - vhost: /app
      configure: ".*"
      write: ".*"
      read: ".*"
`, resourceName, testNamespace, generatedSecretName, resourceName)

			err := utils.ApplyManifest(invalidResource)
			Expect(err).NotTo(HaveOccurred(), "Failed to create invalid RabbitMQAccess resource")

			By("verifying the RabbitMQAccess status reports the reconcile failure")
			Eventually(func(g Gomega) {
				statusCmd := exec.Command(
					"kubectl", "get", "rabbitmqaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
				)
				statusOutput, statusErr := utils.Run(statusCmd)
				g.Expect(statusErr).NotTo(HaveOccurred(), "Failed to retrieve Ready condition status")
				g.Expect(strings.TrimSpace(statusOutput)).To(Equal("False"))

				reasonCmd := exec.Command(
					"kubectl", "get", "rabbitmqaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}",
				)
				reasonOutput, reasonErr := utils.Run(reasonCmd)
				g.Expect(reasonErr).NotTo(HaveOccurred(), "Failed to retrieve Ready condition reason")
				g.Expect(strings.TrimSpace(reasonOutput)).To(Equal("ConnectionError"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the controller logs the expected reconcile error")
			Eventually(func(g Gomega) {
				controllerPodName = ensureControllerPodName()
				logCmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace, "--since=10m")
				logOutput, logErr := utils.Run(logCmd)
				g.Expect(logErr).NotTo(HaveOccurred(), "Failed to read controller logs")
				g.Expect(logOutput).To(ContainSubstring(resourceName))
				g.Expect(logOutput).To(ContainSubstring("no valid connection details provided"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should create a RabbitMQAccess resource and create a RabbitMQ user with the specified permissions via direct connection details", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			resourceName := "test-rabbitmq-access"
			generatedSecret := "test-rabbitmq-credentials"
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a RabbitMQAccess resource")
			err := utils.CreateRabbitMQAccessWithDirectConnection(resourceName, testNamespace, generatedSecret, conn, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource with connection details")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(testNamespace, generatedSecret, "username")

			By("verifying the RabbitMQ user and vhost were created")
			utils.WaitForRabbitMQUserState(testNamespace, resourceName, true)
			utils.WaitForRabbitMQVhostState(testNamespace, "/app", true)

			By("verifying the permissions were granted")
			utils.WaitForRabbitMQPermissions(testNamespace, resourceName, permissions)

			By("verifying the generated credentials can authenticate")
			password := utils.WaitForDecodedSecretField(testNamespace, generatedSecret, "password")
			utils.WaitForRabbitMQAuthenticationSuccess(testNamespace, resourceName, password)
		})

		It("should create a RabbitMQAccess resource with direct host/port and secret-referenced credentials", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			resourceName := "test-rabbitmq-secret-ref"
			generatedSecret := "test-rabbitmq-secret-ref-credentials"
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a secret with the connection details")
			secretName, err := utils.CreateRabbitMQConnectionDetailsViaSecret(testNamespace, conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQ connection secret")

			By("creating a RabbitMQAccess resource referencing the username/password secret and providing host/port directly")
			err = utils.CreateRabbitMQAccessWithConnectionSecretRef(resourceName, testNamespace, generatedSecret, conn, secretName, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource with secret references")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(testNamespace, generatedSecret, "username")

			By("verifying the RabbitMQ user and permissions were created")
			utils.WaitForRabbitMQUserState(testNamespace, resourceName, true)
			utils.WaitForRabbitMQPermissions(testNamespace, resourceName, permissions)
		})

		It("should create a RabbitMQAccess resource using an existing connection secret in the same namespace", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			resourceName := "test-rabbitmq-existing-secret"
			generatedSecret := "test-rabbitmq-existing-secret-credentials"
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a secret with the connection details")
			secretName, err := utils.CreateRabbitMQConnectionDetailsViaSecret(testNamespace, conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQ connection secret")

			By("creating a RabbitMQAccess resource referencing the connection secret")
			err = utils.CreateRabbitMQAccessFromSecretReference(resourceName, testNamespace, generatedSecret, secretName, nil, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource with existingSecret")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(testNamespace, generatedSecret, "username")

			By("verifying the RabbitMQ user and permissions were created")
			utils.WaitForRabbitMQUserState(testNamespace, resourceName, true)
			utils.WaitForRabbitMQPermissions(testNamespace, resourceName, permissions)
		})

		It("should delete the RabbitMQ user and generated secret when the RabbitMQAccess resource is deleted", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			resourceName := "test-rabbitmq-deletion"
			generatedSecret := "test-rabbitmq-deletion-secret"
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app-delete", Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a RabbitMQAccess resource")
			err := utils.CreateRabbitMQAccessWithDirectConnection(resourceName, testNamespace, generatedSecret, conn, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource")

			By("waiting for the generated secret, user, and permissions to exist")
			utils.WaitForSecretField(testNamespace, generatedSecret, "username")
			utils.WaitForRabbitMQUserState(testNamespace, resourceName, true)
			utils.WaitForRabbitMQPermissions(testNamespace, resourceName, permissions)

			By("deleting the RabbitMQAccess resource")
			err = utils.DeleteRabbitMQAccess(resourceName, testNamespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RabbitMQAccess resource")

			By("verifying finalization removed the RabbitMQAccess, user, and generated secret")
			utils.WaitForResourceDeleted("rabbitmqaccess", resourceName, testNamespace)
			utils.WaitForRabbitMQUserState(testNamespace, resourceName, false)
			utils.WaitForSecretDeleted(testNamespace, generatedSecret)
		})

		It("should delete stale RabbitMQ vhosts when singleton Controller policy enables deletion", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			controllerName := "rabbitmq-vhost-cleanup-delete"
			keeperName := "test-rabbitmq-vhost-keeper"
			staleName := "test-rabbitmq-vhost-stale"
			deletePolicy := accessv1.StaleVhostDeletionPolicyDelete
			keeperPermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/keeper", Configure: ".*", Write: ".*", Read: ".*"},
			}
			stalePermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/orphan", Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("enabling stale RabbitMQ vhost deletion through singleton Controller settings")
			err := createRabbitMQController(controllerName, &deletePolicy, nil)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQ controller settings")
			DeferCleanup(func() {
				cmd := exec.Command("kubectl", "delete", "controller", controllerName, "-n", namespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cmd)
			})

			By("creating a keeper RabbitMQAccess resource")
			err = utils.CreateRabbitMQAccessWithDirectConnection(keeperName, testNamespace, "test-rabbitmq-vhost-keeper-secret", conn, keeperPermissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create keeper RabbitMQAccess resource")

			By("creating a second RabbitMQAccess resource that owns an orphanable vhost")
			err = utils.CreateRabbitMQAccessWithDirectConnection(staleName, testNamespace, "test-rabbitmq-vhost-stale-secret", conn, stalePermissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create stale RabbitMQAccess resource")

			By("waiting for both RabbitMQ users and vhosts to exist")
			utils.WaitForRabbitMQUserState(testNamespace, keeperName, true)
			utils.WaitForRabbitMQUserState(testNamespace, staleName, true)
			utils.WaitForRabbitMQVhostState(testNamespace, "/keeper", true)
			utils.WaitForRabbitMQVhostState(testNamespace, "/orphan", true)

			By("deleting the stale RabbitMQAccess resource")
			err = utils.DeleteRabbitMQAccess(staleName, testNamespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete stale RabbitMQAccess resource")

			By("verifying finalization deletes the stale user and its vhost while the keeper remains")
			utils.WaitForResourceDeleted("rabbitmqaccess", staleName, testNamespace)
			utils.WaitForRabbitMQUserState(testNamespace, staleName, false)
			utils.WaitForRabbitMQVhostState(testNamespace, "/orphan", false)
			utils.WaitForRabbitMQUserState(testNamespace, keeperName, true)
			utils.WaitForRabbitMQVhostState(testNamespace, "/keeper", true)
		})

		It("should retain orphaned RabbitMQ vhosts when stale vhost deletion is not enabled", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			keeperName := "test-rabbitmq-vhost-retain-keeper"
			staleName := "test-rabbitmq-vhost-retain-stale"
			keeperPermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/keeper-retain", Configure: ".*", Write: ".*", Read: ".*"},
			}
			stalePermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/retained", Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a keeper RabbitMQAccess resource")
			err := utils.CreateRabbitMQAccessWithDirectConnection(
				keeperName,
				testNamespace,
				"test-rabbitmq-vhost-retain-keeper-secret",
				conn,
				keeperPermissions,
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create keeper RabbitMQAccess resource")

			By("creating a second RabbitMQAccess resource whose vhost should be retained")
			err = utils.CreateRabbitMQAccessWithDirectConnection(
				staleName,
				testNamespace,
				"test-rabbitmq-vhost-retain-stale-secret",
				conn,
				stalePermissions,
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create stale RabbitMQAccess resource")

			By("waiting for both RabbitMQ users and vhosts to exist")
			utils.WaitForRabbitMQUserState(testNamespace, keeperName, true)
			utils.WaitForRabbitMQUserState(testNamespace, staleName, true)
			utils.WaitForRabbitMQVhostState(testNamespace, "/keeper-retain", true)
			utils.WaitForRabbitMQVhostState(testNamespace, "/retained", true)

			By("deleting the stale RabbitMQAccess resource")
			err = utils.DeleteRabbitMQAccess(staleName, testNamespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete stale RabbitMQAccess resource")

			By("verifying finalization deletes the stale user but retains its vhost by policy")
			utils.WaitForResourceDeleted("rabbitmqaccess", staleName, testNamespace)
			utils.WaitForRabbitMQUserState(testNamespace, staleName, false)
			utils.WaitForRabbitMQVhostState(testNamespace, "/retained", true)
			utils.WaitForRabbitMQUserState(testNamespace, keeperName, true)
			utils.WaitForRabbitMQVhostState(testNamespace, "/keeper-retain", true)
		})

		It("should preserve excluded RabbitMQ vhosts when stale vhost deletion is enabled", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			controllerName := "rabbitmq-vhost-cleanup-excluded"
			keeperName := "test-rabbitmq-vhost-excluded-keeper"
			staleName := "test-rabbitmq-vhost-excluded-stale"
			deletePolicy := accessv1.StaleVhostDeletionPolicyDelete
			keeperPermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/keeper-excluded", Configure: ".*", Write: ".*", Read: ".*"},
			}
			stalePermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/protected", Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("enabling stale RabbitMQ vhost deletion while excluding the protected vhost")
			err := createRabbitMQController(controllerName, &deletePolicy, []string{"/protected"})
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQ controller settings")
			DeferCleanup(func() {
				cmd := exec.Command("kubectl", "delete", "controller", controllerName, "-n", namespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cmd)
			})

			By("creating a keeper RabbitMQAccess resource")
			err = utils.CreateRabbitMQAccessWithDirectConnection(
				keeperName,
				testNamespace,
				"test-rabbitmq-vhost-excluded-keeper-secret",
				conn,
				keeperPermissions,
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create keeper RabbitMQAccess resource")

			By("creating a second RabbitMQAccess resource that uses the excluded vhost")
			err = utils.CreateRabbitMQAccessWithDirectConnection(
				staleName,
				testNamespace,
				"test-rabbitmq-vhost-excluded-stale-secret",
				conn,
				stalePermissions,
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create stale RabbitMQAccess resource")

			By("waiting for both RabbitMQ users and vhosts to exist")
			utils.WaitForRabbitMQUserState(testNamespace, keeperName, true)
			utils.WaitForRabbitMQUserState(testNamespace, staleName, true)
			utils.WaitForRabbitMQVhostState(testNamespace, "/keeper-excluded", true)
			utils.WaitForRabbitMQVhostState(testNamespace, "/protected", true)

			By("deleting the stale RabbitMQAccess resource")
			err = utils.DeleteRabbitMQAccess(staleName, testNamespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete stale RabbitMQAccess resource")

			By("verifying finalization deletes the stale user but retains the excluded vhost")
			utils.WaitForResourceDeleted("rabbitmqaccess", staleName, testNamespace)
			utils.WaitForRabbitMQUserState(testNamespace, staleName, false)
			utils.WaitForRabbitMQVhostState(testNamespace, "/protected", true)
			utils.WaitForRabbitMQUserState(testNamespace, keeperName, true)
			utils.WaitForRabbitMQVhostState(testNamespace, "/keeper-excluded", true)
		})

		It("should deny cross-namespace existingSecret when no Controller resource exists", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			connectionSecretNamespace := "rabbitmq-access-shared-no-controller"
			resourceName := "test-rabbitmq-cross-namespace-no-controller"
			generatedSecretName := "test-rabbitmq-cross-namespace-no-controller-secret"
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
			}

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
			secretName, err := utils.CreateRabbitMQConnectionDetailsViaSecret(connectionSecretNamespace, conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RabbitMQAccess that references the shared secret namespace")
			err = utils.CreateRabbitMQAccessFromSecretReference(resourceName, testNamespace, generatedSecretName, secretName, &connectionSecretNamespace, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace RabbitMQAccess")

			By("verifying reconcile is denied with cross-namespace policy disabled")
			Eventually(func(g Gomega) {
				statusCmd := exec.Command(
					"kubectl", "get", "rabbitmqaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
				)
				statusOutput, statusErr := utils.Run(statusCmd)
				g.Expect(statusErr).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(statusOutput)).To(Equal("False"))

				reasonCmd := exec.Command(
					"kubectl", "get", "rabbitmqaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}",
				)
				reasonOutput, reasonErr := utils.Run(reasonCmd)
				g.Expect(reasonErr).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(reasonOutput)).To(Equal("ConnectionError"))

				messageCmd := exec.Command(
					"kubectl", "get", "rabbitmqaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].message}",
				)
				messageOutput, messageErr := utils.Run(messageCmd)
				g.Expect(messageErr).NotTo(HaveOccurred())
				g.Expect(messageOutput).To(ContainSubstring("cross-namespace connection secret references are disabled"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the requested RabbitMQ user was not created")
			utils.WaitForRabbitMQUserState(testNamespace, resourceName, false)
		})

		It("should deny cross-namespace existingSecret when singleton Controller setting is false", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			connectionSecretNamespace := "rabbitmq-access-shared-controller-false"
			controllerName := "rabbitmq-cluster-settings-false"
			resourceName := "test-rabbitmq-cross-namespace-controller-false"
			generatedSecretName := "test-rabbitmq-cross-namespace-controller-false-secret"
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
			}

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
			err := utils.ApplyManifest(controllerYAML)
			Expect(err).NotTo(HaveOccurred(), "Failed to create singleton Controller with false policy")
			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "controller", controllerName, "-n", namespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating a shared namespace for the connection secret")
			cmd := exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--timeout=1m")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "create", "ns", connectionSecretNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create shared secret namespace")
			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating the connection secret in another namespace")
			secretName, err := utils.CreateRabbitMQConnectionDetailsViaSecret(connectionSecretNamespace, conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RabbitMQAccess that references the shared secret namespace")
			err = utils.CreateRabbitMQAccessFromSecretReference(resourceName, testNamespace, generatedSecretName, secretName, &connectionSecretNamespace, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace RabbitMQAccess")

			By("verifying reconcile is denied because singleton Controller policy is false")
			Eventually(func(g Gomega) {
				messageCmd := exec.Command(
					"kubectl", "get", "rabbitmqaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].message}",
				)
				messageOutput, messageErr := utils.Run(messageCmd)
				g.Expect(messageErr).NotTo(HaveOccurred())
				g.Expect(messageOutput).To(ContainSubstring("cross-namespace connection secret references are disabled"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should create a RabbitMQAccess resource using an existing connection secret from another namespace", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			connectionSecretNamespace := fmt.Sprintf("%s-shared", testNamespace)
			controllerName := "rabbitmq-cluster-settings"
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
			}

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
			err := utils.ApplyManifest(controllerYAML)
			Expect(err).NotTo(HaveOccurred(), "Failed to enable cross-namespace references via Controller CR")
			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "controller", controllerName, "-n", namespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("resetting the shared secret namespace used for cross-namespace secret references")
			cmd := exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--timeout=1m")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "create", "ns", connectionSecretNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create shared secret namespace")
			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating the connection secret in the shared namespace")
			secretName, err := utils.CreateRabbitMQConnectionDetailsViaSecret(connectionSecretNamespace, conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RabbitMQAccess resource in the workload namespace that references the shared secret")
			err = utils.CreateRabbitMQAccessFromSecretReference(
				"test-rabbitmq-cross-namespace",
				testNamespace,
				"test-rabbitmq-cross-namespace-credentials",
				secretName,
				&connectionSecretNamespace,
				permissions,
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource with cross-namespace secret reference")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(testNamespace, "test-rabbitmq-cross-namespace-credentials", "username")

			By("verifying the RabbitMQ user and permissions were created")
			utils.WaitForRabbitMQUserState(testNamespace, "test-rabbitmq-cross-namespace", true)
			utils.WaitForRabbitMQPermissions(testNamespace, "test-rabbitmq-cross-namespace", permissions)
		})

		It("should fail when multiple Controller resources exist and emit warning events", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			connectionSecretNamespace := "rabbitmq-access-shared-multiple-controller"
			controllerAName := "rabbitmq-cluster-settings-a"
			controllerBName := "rabbitmq-cluster-settings-b"
			resourceName := "test-rabbitmq-multiple-controllers"
			generatedSecretName := "test-rabbitmq-multiple-controllers-secret"
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
			}

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
			err := utils.ApplyManifest(controllerAYAML)
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
			err = utils.ApplyManifest(controllerBYAML)
			Expect(err).NotTo(HaveOccurred(), "Failed to create second Controller")

			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "controller", controllerAName, "-n", namespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating a shared namespace for the connection secret")
			cmd := exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--timeout=1m")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "create", "ns", connectionSecretNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create shared secret namespace")
			DeferCleanup(func() {
				cleanupCmd := exec.Command("kubectl", "delete", "ns", connectionSecretNamespace, "--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cleanupCmd)
			})

			By("creating the connection secret in another namespace")
			secretName, err := utils.CreateRabbitMQConnectionDetailsViaSecret(connectionSecretNamespace, conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RabbitMQAccess that references the shared secret namespace")
			err = utils.CreateRabbitMQAccessFromSecretReference(resourceName, testNamespace, generatedSecretName, secretName, &connectionSecretNamespace, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace RabbitMQAccess")

			By("verifying RabbitMQAccess fails with multiple-controller error")
			Eventually(func(g Gomega) {
				reasonCmd := exec.Command(
					"kubectl", "get", "rabbitmqaccess", resourceName, "-n", testNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}",
				)
				reasonOutput, reasonErr := utils.Run(reasonCmd)
				g.Expect(reasonErr).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(reasonOutput)).To(Equal("ConnectionError"))

				messageCmd := exec.Command(
					"kubectl", "get", "rabbitmqaccess", resourceName, "-n", testNamespace,
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

		It("should reconcile permissions when they're changed in the config", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			resourceName := "test-rabbitmq-permissions-reconciliation"
			generatedSecret := "test-rabbitmq-permissions-reconciliation-secret"
			initialPermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: "^$", Write: "^$", Read: "^$"},
			}
			updatedPermissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a RabbitMQAccess resource with certain permissions")
			err := utils.CreateRabbitMQAccessWithDirectConnection(resourceName, testNamespace, generatedSecret, conn, initialPermissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource")

			By("waiting for the initial permissions to be granted")
			utils.WaitForRabbitMQPermissions(testNamespace, resourceName, initialPermissions)

			By("updating the RabbitMQAccess resource to include new permissions")
			err = utils.CreateRabbitMQAccessWithDirectConnection(resourceName, testNamespace, generatedSecret, conn, updatedPermissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to update RabbitMQAccess resource")

			By("verifying that the new permissions are granted")
			utils.WaitForRabbitMQPermissions(testNamespace, resourceName, updatedPermissions)
		})

		It("should reconcile the permissions of a RabbitMQAccess resource when they are manually revoked", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			resourceName := "test-rabbitmq-permissions-maintenance"
			generatedSecret := "test-rabbitmq-permissions-maintenance-secret"
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a RabbitMQAccess resource")
			err := utils.CreateRabbitMQAccessWithDirectConnection(resourceName, testNamespace, generatedSecret, conn, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource")

			By("waiting for the permissions to be granted")
			utils.WaitForRabbitMQPermissions(testNamespace, resourceName, permissions)

			By("revoking the permissions from the RabbitMQ user")
			_, err = utils.RunRabbitMQctl(testNamespace, "clear_permissions", "-p", "/app", resourceName)
			Expect(err).NotTo(HaveOccurred(), "Failed to clear RabbitMQ permissions")

			err = utils.TriggerReconciliation("rabbitmqaccess", resourceName, testNamespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to trigger reconciliation after clearing permissions")

			By("verifying that the controller reconciles and restores the permissions")
			utils.WaitForRabbitMQPermissions(testNamespace, resourceName, permissions)
		})

		It("should update the RabbitMQ user's password when the secret's password is rolled via deletion", func() {
			testNamespace, conn := utils.GetRabbitMQVariables()
			resourceName := "test-rabbitmq-password-rotation"
			generatedSecret := "test-rabbitmq-password-rotation-secret"
			permissions := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
			}

			By("creating a RabbitMQAccess resource")
			err := utils.CreateRabbitMQAccessWithDirectConnection(resourceName, testNamespace, generatedSecret, conn, permissions)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RabbitMQAccess resource")

			By("waiting for the generated secret and initial authentication")
			oldPassword := utils.WaitForDecodedSecretField(testNamespace, generatedSecret, "password")
			utils.WaitForRabbitMQAuthenticationSuccess(testNamespace, resourceName, oldPassword)

			By("deleting the generated secret to trigger password rotation")
			cmd := exec.Command("kubectl", "delete", "secret", generatedSecret, "-n", testNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete generated secret")

			By("verifying that the RabbitMQ user's password is rotated and the new password authenticates")
			newPassword := utils.WaitForDecodedSecretField(testNamespace, generatedSecret, "password")
			Expect(newPassword).NotTo(Equal(oldPassword))
			utils.WaitForRabbitMQAuthenticationSuccess(testNamespace, resourceName, newPassword)
		})
	})
})

func createRabbitMQController(
	name string,
	policy *accessv1.StaleVhostDeletionPolicy,
	excludedVhosts []string,
) error {
	var manifest strings.Builder
	manifest.WriteString(fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: Controller
metadata:
  name: %s
  namespace: %s
spec:
  settings:
`, name, namespace))

	if policy != nil || len(excludedVhosts) > 0 {
		manifest.WriteString("    rabbitmq:\n")
		if len(excludedVhosts) > 0 {
			manifest.WriteString("      excludedVhosts:\n")
			for _, vhost := range excludedVhosts {
				manifest.WriteString(fmt.Sprintf("        - %s\n", vhost))
			}
		}
		if policy != nil {
			manifest.WriteString(fmt.Sprintf("      staleVhostDeletionPolicy: %s\n", *policy))
		}
	}

	return utils.ApplyManifest(manifest.String())
}
