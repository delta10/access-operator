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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/delta10/access-operator/internal/controller"
	"github.com/delta10/access-operator/test/utils"
)

var _ = Describe("Redis", func() {
	Context("Redis", func() {
		var env redisSpecEnv

		BeforeEach(func() {
			env = newRedisSpecEnv()
		})

		AfterEach(func() {
			env.cleanup()
		})

		It("should log reconcile errors and set Ready=False when connection details are invalid", func() {
			resourceName := env.name("invalid-redis-connection")
			generatedSecretName := env.name("invalid-redis-connection-secret")

			By("creating a RedisAccess resource with invalid connection details")
			invalidResource := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: RedisAccess
metadata:
  name: %s
  namespace: %s
spec:
  generatedSecret: %s
  username: %s
  connection: {}
  aclRules:
    - "~cache:*"
    - "+get"
`, resourceName, env.namespace, generatedSecretName, resourceName)

			err := utils.ApplyManifest(invalidResource)
			Expect(err).NotTo(HaveOccurred(), "Failed to create invalid RedisAccess resource")

			By("verifying the RedisAccess status reports the reconcile failure")
			waitForReadyCondition("redisaccess", namespacedName{name: resourceName, namespace: env.namespace}, readyConditionExpectation{
				status: "False",
				reason: "ConnectionError",
			})

			By("verifying the controller logs the expected reconcile error")
			waitForControllerLogsContain(resourceName, "failed to resolve Redis connection details", "no valid connection details provided")
		})

		It("should create a RedisAccess resource and create a Redis ACL user via direct connection details", func() {
			resourceName := env.name("test-redis-access")
			generatedSecret := env.name("test-redis-credentials")
			aclRules := []string{"~cache:*", "+@read", "+@write"}

			By("creating a RedisAccess resource")
			err := utils.CreateRedisAccessWithDirectConnection(resourceName, env.namespace, generatedSecret, env.conn, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RedisAccess resource with connection details")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the Redis ACL user was created")
			utils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, true)
			utils.WaitForRedisACLRules(env.backendNamespace, env.conn, resourceName, aclRules)

			By("verifying the generated credentials can authenticate")
			password := utils.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")
			utils.WaitForRedisAuthenticationSuccess(env.backendNamespace, resourceName, password)
		})

		It("should create a RedisAccess resource with direct host/port and secret-referenced credentials", func() {
			resourceName := env.name("test-redis-secret-ref")
			generatedSecret := env.name("test-redis-secret-ref-credentials")
			aclRules := []string{"~orders:*", "+get", "+set"}

			By("creating a secret with the connection details")
			secretName, err := utils.CreateRedisConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create Redis connection secret")

			By("creating a RedisAccess resource referencing the username/password secret and providing host/port directly")
			err = utils.CreateRedisAccessWithConnectionSecretRef(resourceName, env.namespace, generatedSecret, env.conn, secretName, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RedisAccess resource with secret references")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the Redis ACL user and rules were created")
			utils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, true)
			utils.WaitForRedisACLRules(env.backendNamespace, env.conn, resourceName, aclRules)
		})

		It("should create a RedisAccess resource using an existing connection secret in the same namespace", func() {
			resourceName := env.name("test-redis-existing-secret")
			generatedSecret := env.name("test-redis-existing-secret-credentials")
			aclRules := []string{"~shared:*", "+get"}

			By("creating a secret with the connection details")
			secretName, err := utils.CreateRedisConnectionDetailsViaSecret(env.namespace, env.conn)
			Expect(err).NotTo(HaveOccurred(), "Failed to create Redis connection secret")

			By("creating a RedisAccess resource referencing the connection secret")
			err = utils.CreateRedisAccessFromSecretReference(resourceName, env.namespace, generatedSecret, secretName, nil, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RedisAccess resource with existingSecret")

			By("waiting for the generated secret to be created")
			utils.WaitForSecretField(env.namespace, generatedSecret, "username")

			By("verifying the Redis ACL user and rules were created")
			utils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, true)
			utils.WaitForRedisACLRules(env.backendNamespace, env.conn, resourceName, aclRules)
		})

		It("should delete the Redis ACL user and generated secret when the RedisAccess resource is deleted", func() {
			resourceName := env.name("test-redis-deletion")
			generatedSecret := env.name("test-redis-deletion-secret")
			aclRules := []string{"~delete:*", "+get", "+set"}

			By("creating a RedisAccess resource")
			err := utils.CreateRedisAccessWithDirectConnection(resourceName, env.namespace, generatedSecret, env.conn, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RedisAccess resource")

			By("waiting for the generated secret and Redis user to exist")
			utils.WaitForSecretField(env.namespace, generatedSecret, "username")
			utils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, true)

			By("deleting the RedisAccess resource")
			err = utils.DeleteRedisAccess(resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RedisAccess resource")

			By("verifying finalization removed the RedisAccess, Redis user, and generated secret")
			utils.WaitForResourceDeleted("redisaccess", resourceName, env.namespace)
			utils.WaitForRedisUserState(env.backendNamespace, env.conn, resourceName, false)
			utils.WaitForSecretDeleted(env.namespace, generatedSecret)
		})

		It("should reconcile Redis ACL rules when they are manually drifted", func() {
			resourceName := env.name("test-redis-drift")
			generatedSecret := env.name("test-redis-drift-secret")
			desiredRules := []string{"~drift:*", "+get", "+set"}

			By("creating a RedisAccess resource")
			err := utils.CreateRedisAccessWithDirectConnection(resourceName, env.namespace, generatedSecret, env.conn, desiredRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RedisAccess resource")

			By("waiting for the generated secret and initial Redis ACL rules")
			utils.WaitForSecretField(env.namespace, generatedSecret, "username")
			utils.WaitForRedisACLRules(env.backendNamespace, env.conn, resourceName, desiredRules)

			By("manually drifting the Redis ACL user")
			_, err = utils.RunRedisCLI(env.backendNamespace, env.conn,
				"ACL", "SETUSER", resourceName,
				"reset", "on", ">wrong-password", "~drifted:*", "+get",
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to drift Redis ACL user")

			err = utils.TriggerReconciliation("redisaccess", resourceName, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to trigger Redis reconciliation")

			By("verifying that the controller restores the desired Redis ACL rules and password")
			utils.WaitForRedisACLRules(env.backendNamespace, env.conn, resourceName, desiredRules)
			password := utils.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")
			utils.WaitForRedisAuthenticationSuccess(env.backendNamespace, resourceName, password)
		})

		It("should update the Redis user's password when the generated secret is rotated by deletion", func() {
			resourceName := env.name("test-redis-password-rotation")
			generatedSecret := env.name("test-redis-password-rotation-secret")
			aclRules := []string{"~rotation:*", "+get"}

			By("creating a RedisAccess resource")
			err := utils.CreateRedisAccessWithDirectConnection(resourceName, env.namespace, generatedSecret, env.conn, aclRules)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RedisAccess resource")

			By("waiting for the initial generated credentials")
			oldPassword := utils.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")
			utils.WaitForRedisAuthenticationSuccess(env.backendNamespace, resourceName, oldPassword)

			By("deleting the generated secret to trigger password rotation")
			cmd := exec.Command("kubectl", "delete", "secret", generatedSecret, "-n", env.namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete generated secret")

			By("verifying that the Redis user's password is rotated and the new password authenticates")
			newPassword := utils.WaitForDecodedSecretField(env.namespace, generatedSecret, "password")
			Expect(newPassword).NotTo(Equal(oldPassword))
			utils.WaitForRedisAuthenticationSuccess(env.backendNamespace, resourceName, newPassword)
		})

		It("should keep Redis ACL users that are still used as connection usernames by another RedisAccess", func() {
			protectedUser := env.name("test-redis-protected-user")
			protectedSecret := env.name("test-redis-protected-user-secret")
			dependentResource := env.name("test-redis-dependent")
			dependentSecret := env.name("test-redis-dependent-secret")

			By("creating the RedisAccess that owns the connection username")
			err := utils.CreateRedisAccessWithDirectConnection(protectedUser, env.namespace, protectedSecret, env.conn, []string{"~*", "+@all"})
			Expect(err).NotTo(HaveOccurred(), "Failed to create protected RedisAccess")

			By("waiting for the protected user credentials to exist")
			utils.WaitForSecretField(env.namespace, protectedSecret, "username")
			protectedPassword := utils.WaitForDecodedSecretField(env.namespace, protectedSecret, "password")
			utils.WaitForRedisAuthenticationSuccess(env.backendNamespace, protectedUser, protectedPassword)

			By("creating another RedisAccess that uses the protected user for its Redis connection")
			err = utils.CreateRedisAccessWithConnectionSecretRef(dependentResource, env.namespace, dependentSecret, controller.ConnectionDetails{
				SharedConnectionDetails: controller.SharedConnectionDetails{
					Host: env.conn.Host,
					Port: env.conn.Port,
				},
			}, protectedSecret, []string{"~dependent:*", "+get"})
			Expect(err).NotTo(HaveOccurred(), "Failed to create dependent RedisAccess")

			By("waiting for the dependent resource to reconcile")
			utils.WaitForSecretField(env.namespace, dependentSecret, "username")
			utils.WaitForRedisUserState(env.backendNamespace, env.conn, dependentResource, true)

			By("deleting the RedisAccess that owns the connection username")
			err = utils.DeleteRedisAccess(protectedUser, env.namespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete protected RedisAccess")

			By("verifying the protected Redis user is retained because another RedisAccess still uses it for connections")
			utils.WaitForResourceDeleted("redisaccess", protectedUser, env.namespace)
			utils.WaitForRedisUserState(env.backendNamespace, env.conn, protectedUser, true)
			utils.WaitForSecretDeleted(env.namespace, protectedSecret)
		})
	})
})
