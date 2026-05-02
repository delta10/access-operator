//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"strings"

	accessv1 "github.com/delta10/access-operator/api/v1"
	e2eutils "github.com/delta10/access-operator/test/e2e/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Redis", func() {
	Context("Settings ConfigMap policy", func() {
		var env redisSpecEnv

		BeforeEach(func() {
			env = newRedisSpecEnv()
		})

		AfterEach(func() {
			env.cleanup()
		})

		It("should deny cross-namespace existingSecret when no settings ConfigMap exists", func() {
			resourceName := env.name("test-redis-cross-namespace-no-controller")
			generatedSecretName := env.name("test-redis-cross-namespace-no-controller-secret")
			connectionSecretNamespace := createTestNamespace("redis-shared-no-controller")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})
			aclRules := []string{"~shared:*", "+get"}

			By("creating the connection secret in another namespace")
			secretName, err := e2eutils.CreateRedisConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RedisAccess that references the shared secret namespace")
			err = e2eutils.CreateRedisAccessFromSecretReference(resourceName, env.namespace, generatedSecretName, secretName, &connectionSecretNamespace, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace RedisAccess")

			By("verifying reconcile is denied with cross-namespace policy disabled")
			waitForReadyCondition("redisaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				status:          "False",
				reason:          "ConnectionError",
				messageContains: "cross-namespace connection secret references are disabled",
			})

			By("verifying the requested Redis ACL user was not created")
			e2eutils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, false)
		})

		It("should deny cross-namespace existingSecret when settings ConfigMap setting is false", Serial, func() {
			clearAllControllerSettingsConfigMaps()
			DeferCleanup(clearAllControllerSettingsConfigMaps)

			resourceName := env.name("test-redis-cross-namespace-controller-false")
			generatedSecretName := env.name("test-redis-cross-namespace-controller-false-secret")
			connectionSecretNamespace := createTestNamespace("redis-shared-controller-false")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})
			aclRules := []string{"~shared:*", "+get"}

			By("creating settings ConfigMap with existingSecretNamespace=false")
			err := createControllerSettingsConfigMap(namespace, `existingSecretNamespace: false`)
			Expect(err).NotTo(HaveOccurred(), "Failed to create settings ConfigMap with false policy")

			By("creating the connection secret in another namespace")
			secretName, err := e2eutils.CreateRedisConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RedisAccess that references the shared secret namespace")
			err = e2eutils.CreateRedisAccessFromSecretReference(resourceName, env.namespace, generatedSecretName, secretName, &connectionSecretNamespace, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create cross-namespace RedisAccess")

			By("verifying reconcile is denied because settings ConfigMap policy is false")
			waitForReadyCondition("redisaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				messageContains: "cross-namespace connection secret references are disabled",
			})

			By("verifying the requested Redis ACL user was not created")
			e2eutils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, false)
		})

		It("should create a RedisAccess resource using an existing connection secret from another namespace", Serial, func() {
			clearAllControllerSettingsConfigMaps()
			DeferCleanup(clearAllControllerSettingsConfigMaps)

			resourceName := env.name("test-redis-cross-namespace")
			generatedSecretName := env.name("test-redis-cross-namespace-credentials")
			connectionSecretNamespace := createTestNamespace("redis-shared")
			DeferCleanup(func() {
				deleteNamespace(connectionSecretNamespace)
			})
			aclRules := []string{"~cross:*", "+get", "+set"}

			By("enabling cross-namespace references through operator settings ConfigMap")
			err := createControllerSettingsConfigMap(namespace, `existingSecretNamespace: true`)
			Expect(err).NotTo(HaveOccurred(), "Failed to enable cross-namespace references via Controller CR")

			By("creating the connection secret in the shared namespace")
			secretName, err := e2eutils.CreateRedisConnectionDetailsViaSecret(connectionSecretNamespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create connection secret in shared namespace")

			By("creating a RedisAccess resource in the workload namespace that references the shared secret")
			err = e2eutils.CreateRedisAccessFromSecretReference(resourceName, env.namespace, generatedSecretName, secretName, &connectionSecretNamespace, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RedisAccess resource with cross-namespace secret reference")

			By("waiting for the generated secret to be created")
			e2eutils.WaitForSecretField(env.namespace, generatedSecretName, "username")

			By("verifying the Redis ACL user and rules were created")
			e2eutils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, true)
			e2eutils.WaitForRedisACLRules(env.backendNamespace, env.conn, resourceName, aclRules)
		})

		It("should preserve excluded Redis ACL users from settings ConfigMap", Serial, func() {
			clearAllControllerSettingsConfigMaps()
			DeferCleanup(clearAllControllerSettingsConfigMaps)

			excludedUsername := env.name("excluded-keeper")
			managedUsername := env.name("test-redis-managed-user")
			generatedSecret := env.name("test-redis-managed-secret")
			managedACLRules := []string{"~managed:*", "+get"}

			settingsYAML := strings.Join([]string{
				"redis:",
				"  excludedUsers:",
				fmt.Sprintf("    - %s", excludedUsername),
			}, "\n")

			By("creating settings ConfigMap that excludes the unmanaged Redis user")
			err := createControllerSettingsConfigMap(namespace, settingsYAML)
			Expect(err).NotTo(HaveOccurred(), "Failed to create Redis exclusion Controller")

			By("creating an unmanaged Redis ACL user that should be preserved")
			_, err = e2eutils.RunRedisCLI(
				env.backendNamespace,
				env.conn,
				"ACL", "SETUSER", excludedUsername,
				"reset", "on", ">keep-me", "~excluded:*", "+get",
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create excluded Redis ACL user")

			By("creating a RedisAccess resource to trigger reconciliation")
			err = e2eutils.CreateRedisAccessWithDirectConnection(managedUsername, env.namespace, generatedSecret, env.conn, managedACLRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RedisAccess resource")

			By("waiting for the managed Redis ACL user to exist")
			e2eutils.WaitForRedisUserState(env.backendNamespace, env.conn, managedUsername, true)

			By("verifying the excluded unmanaged Redis user is not removed")
			e2eutils.WaitForRedisUserState(env.backendNamespace, env.conn, excludedUsername, true)
		})

		It("should retain stale Redis users when stale user deletion policy is Restrict", func() {
			resourceName := env.name("test-redis-restrict-retain")
			generatedSecret := env.name("test-redis-restrict-retain-secret")
			aclRules := []string{"~retain:*", "+get"}

			By("creating a RedisAccess resource")
			err := e2eutils.CreateRedisAccessWithDirectConnection(resourceName, env.namespace, generatedSecret, env.conn, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RedisAccess resource")

			By("waiting for the Redis ACL user to exist")
			e2eutils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, true)

			By("deleting the RedisAccess resource")
			err = e2eutils.DeleteRedisAccess(resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RedisAccess resource")

			By("verifying the Redis user is retained by the default Restrict policy")
			e2eutils.WaitForResourceDeleted("redisaccess", resourceName, env.namespace)
			e2eutils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, true)
			e2eutils.WaitForSecretDeleted(env.namespace, generatedSecret)
		})

		It("should delete stale Redis users when stale user deletion policy is Delete", Serial, func() {
			clearAllControllerSettingsConfigMaps()
			DeferCleanup(clearAllControllerSettingsConfigMaps)

			resourceName := env.name("test-redis-delete-stale-user")
			generatedSecret := env.name("test-redis-delete-stale-user-secret")
			deletePolicy := accessv1.StaleUserDeletionPolicyDelete
			aclRules := []string{"~delete:*", "+get"}

			By("creating a settings ConfigMap with staleUserDeletionPolicy Delete")
			err := createControllerSettingsConfigMap(namespace, fmt.Sprintf(`redis:
  staleUserDeletionPolicy: %s`, deletePolicy))
			Expect(err).NotTo(HaveOccurred(), "Failed to create Redis controller settings")
			DeferCleanup(func() {
				deleteControllerSettingsConfigMap(namespace)
			})

			By("creating a RedisAccess resource")
			err = e2eutils.CreateRedisAccessWithDirectConnection(resourceName, env.namespace, generatedSecret, env.conn, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RedisAccess resource")

			By("waiting for the Redis ACL user to exist")
			e2eutils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, true)

			By("deleting the RedisAccess resource")
			err = e2eutils.DeleteRedisAccess(resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RedisAccess resource")

			By("verifying the Redis user is deleted by controller policy")
			e2eutils.WaitForResourceDeleted("redisaccess", resourceName, env.namespace)
			e2eutils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, false)
			e2eutils.WaitForSecretDeleted(env.namespace, generatedSecret)
		})
	})
})
