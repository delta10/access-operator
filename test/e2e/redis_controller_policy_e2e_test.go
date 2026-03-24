//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/delta10/access-operator/test/utils"
)

var _ = Describe("Redis", func() {
	Context("Controller policy", Serial, func() {
		var env redisSpecEnv

		BeforeEach(func() {
			clearAllControllers()
			env = newRedisSpecEnv()
		})

		AfterEach(func() {
			env.cleanup()
			clearAllControllers()
		})

		It("should deny cross-namespace existingSecret when no Controller resource exists", func() {
			resourceName := env.name("test-redis-cross-namespace-no-controller")
			generatedSecretName := env.name("test-redis-cross-namespace-no-controller-secret")
			connectionSecretNamespace := createTestNamespace("redis-shared-no-controller")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})
			aclRules := []string{"~shared:*", "+get"}

			By("creating the connection secret in another namespace")
			secretName, err := utils.CreateRedisConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RedisAccess that references the shared secret namespace")
			err = utils.CreateRedisAccessFromSecretReference(resourceName, env.namespace, generatedSecretName, secretName, &connectionSecretNamespace, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace RedisAccess")

			By("verifying reconcile is denied with cross-namespace policy disabled")
			waitForReadyCondition("redisaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				status:          "False",
				reason:          "ConnectionError",
				messageContains: "cross-namespace connection secret references are disabled",
			})

			By("verifying the requested Redis ACL user was not created")
			utils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, false)
		})

		It("should deny cross-namespace existingSecret when singleton Controller setting is false", func() {
			resourceName := env.name("test-redis-cross-namespace-controller-false")
			generatedSecretName := env.name("test-redis-cross-namespace-controller-false-secret")
			controllerName := env.name("redis-cluster-settings-false")
			connectionSecretNamespace := createTestNamespace("redis-shared-controller-false")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})
			aclRules := []string{"~shared:*", "+get"}

			By("creating a singleton Controller with existingSecretNamespace=false")
			err := createControllerResource(controllerName, namespace, `existingSecretNamespace: false`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create singleton Controller with false policy")

			By("creating the connection secret in another namespace")
			secretName, err := utils.CreateRedisConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RedisAccess that references the shared secret namespace")
			err = utils.CreateRedisAccessFromSecretReference(resourceName, env.namespace, generatedSecretName, secretName, &connectionSecretNamespace, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace RedisAccess")

			By("verifying reconcile is denied because singleton Controller policy is false")
			waitForReadyCondition("redisaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				messageContains: "cross-namespace connection secret references are disabled",
			})

			By("verifying the requested Redis ACL user was not created")
			utils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, false)
		})

		It("should create a RedisAccess resource using an existing connection secret from another namespace", func() {
			resourceName := env.name("test-redis-cross-namespace")
			generatedSecretName := env.name("test-redis-cross-namespace-credentials")
			controllerName := env.name("redis-cluster-settings")
			connectionSecretNamespace := createTestNamespace("redis-shared")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})
			aclRules := []string{"~cross:*", "+get", "+set"}

			By("enabling cross-namespace references through the singleton Controller resource")
			err := createControllerResource(controllerName, namespace, `existingSecretNamespace: true`)
			Expect(err).NotTo(HaveOccurred(), "Failed to enable cross-namespace references via Controller CR")

			By("creating the connection secret in the shared namespace")
			secretName, err := utils.CreateRedisConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RedisAccess resource in the workload namespace that references the shared secret")
			err = utils.CreateRedisAccessFromSecretReference(resourceName, env.namespace, generatedSecretName, secretName, &connectionSecretNamespace, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RedisAccess resource with cross-namespace secret reference")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(env.namespace, generatedSecretName, "username")

			By("verifying the Redis ACL user and rules were created")
			utils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, true)
			utils.WaitForRedisACLRules(env.backendNamespace, env.conn, resourceName, aclRules)
		})

		It("should preserve excluded Redis ACL users from singleton Controller settings", func() {
			excludedUsername := env.name("excluded-keeper")
			managedUsername := env.name("test-redis-managed-user")
			generatedSecret := env.name("test-redis-managed-secret")
			controllerName := env.name("redis-excluded-users")
			managedACLRules := []string{"~managed:*", "+get"}

			settingsYAML := strings.Join([]string{
				"redis:",
				"  excludedUsers:",
				fmt.Sprintf("    - %s", excludedUsername),
			}, "\n")

			By("creating a singleton Controller that excludes the unmanaged Redis user")
			err := createControllerResource(controllerName, namespace, settingsYAML)
			Expect(err).NotTo(HaveOccurred(), "Failed to create Redis exclusion Controller")

			By("creating an unmanaged Redis ACL user that should be preserved")
			_, err = utils.RunRedisCLI(
				env.backendNamespace,
				env.conn,
				"ACL", "SETUSER", excludedUsername,
				"reset", "on", ">keep-me", "~excluded:*", "+get",
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create excluded Redis ACL user")

			By("creating a RedisAccess resource to trigger reconciliation")
			err = utils.CreateRedisAccessWithDirectConnection(managedUsername, env.namespace, generatedSecret, env.conn, managedACLRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RedisAccess resource")

			By("waiting for the managed Redis ACL user to exist")
			utils.WaitForRedisUserState(env.backendNamespace, env.conn, managedUsername, true)

			By("verifying the excluded unmanaged Redis user is not removed")
			utils.WaitForRedisUserState(env.backendNamespace, env.conn, excludedUsername, true)
		})
	})
})
