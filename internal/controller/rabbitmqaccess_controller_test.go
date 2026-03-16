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

package controller

import (
	"context"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

var _ = Describe("RabbitMQAccess Controller", func() {
	Context("When calculating stale RabbitMQ permissions", func() {
		It("should return vhosts that are no longer desired", func() {
			current := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
				{VHost: "/old", Configure: ".*", Write: ".*", Read: ".*"},
			}
			desired := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: "^$", Write: "^$", Read: "^$"},
			}

			Expect(stalePermissionVHosts(current, desired)).To(Equal([]string{"/old"}))
		})

		It("should not mark a vhost as stale when only the regexes change", func() {
			current := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: "^$", Write: "^$", Read: "^$"},
			}
			desired := []accessv1.RabbitMQPermissionSpec{
				{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
			}

			Expect(stalePermissionVHosts(current, desired)).To(BeEmpty())
		})
	})

	Context("When resolving excluded RabbitMQ users", func() {
		It("should normalize excluded usernames from singleton Controller settings", func() {
			fakeClient, _ := newFakeClientWithScheme(
				&accessv1.Controller{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cluster-settings",
						Namespace: "access-operator-system",
					},
					Spec: accessv1.ControllerSpec{
						Settings: accessv1.ControllerSettings{
							RabbitMQSettings: accessv1.RabbitMQControllerSettings{
								ExcludedUsers: []string{" admin ", "", "ops-user", "admin"},
							},
						},
					},
				},
			)

			reconciler := &RabbitMQAccessReconciler{Client: fakeClient}
			excludedUsers, err := reconciler.resolveExcludedUsers(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(excludedUsers).To(HaveLen(2))
			Expect(excludedUsers).To(HaveKey("admin"))
			Expect(excludedUsers).To(HaveKey("ops-user"))
		})
	})

	Context("When resolving excluded RabbitMQ vhosts", func() {
		It("should normalize excluded vhosts and always retain the default vhost", func() {
			fakeClient, _ := newFakeClientWithScheme(
				&accessv1.Controller{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cluster-settings",
						Namespace: "access-operator-system",
					},
					Spec: accessv1.ControllerSpec{
						Settings: accessv1.ControllerSettings{
							RabbitMQSettings: accessv1.RabbitMQControllerSettings{
								ExcludedVhosts: []string{" /shared ", "", "/team-a", "/shared"},
							},
						},
					},
				},
			)

			reconciler := &RabbitMQAccessReconciler{Client: fakeClient}
			excludedVhosts, err := reconciler.resolveExcludedVhosts(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(excludedVhosts).To(HaveLen(3))
			Expect(excludedVhosts).To(HaveKey("/"))
			Expect(excludedVhosts).To(HaveKey("/shared"))
			Expect(excludedVhosts).To(HaveKey("/team-a"))
		})
	})

	Context("When calculating stale RabbitMQ vhosts", func() {
		It("should only delete orphaned vhosts when deletion is enabled", func() {
			currentVhosts := []string{"/", "/app", "/shared", "/admin", "/orphan"}
			desiredUsers := map[string]RabbitMQUserConfig{
				"app-user": {
					Permissions: []accessv1.RabbitMQPermissionSpec{
						{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
					},
				},
			}
			currentPermissions := map[string][]accessv1.RabbitMQPermissionSpec{
				"admin": {
					{VHost: "/admin", Configure: ".*", Write: ".*", Read: ".*"},
				},
				"ops-user": {
					{VHost: "/shared", Configure: ".*", Write: ".*", Read: ".*"},
				},
				"stale-user": {
					{VHost: "/orphan", Configure: ".*", Write: ".*", Read: ".*"},
				},
			}
			excludedUsers := map[string]struct{}{
				"ops-user": {},
			}
			excludedVhosts := map[string]struct{}{
				"/": {},
			}

			stale := staleRabbitMQVhosts(
				currentVhosts,
				desiredUsers,
				currentPermissions,
				excludedUsers,
				excludedVhosts,
				accessv1.StaleVhostDeletionPolicyDelete,
			)

			Expect(stale).To(Equal([]string{"/admin", "/orphan"}))
		})

		It("should retain stale vhosts when deletion is disabled", func() {
			stale := staleRabbitMQVhosts(
				[]string{"/orphan"},
				nil,
				nil,
				nil,
				map[string]struct{}{"/": {}},
				accessv1.StaleVhostDeletionPolicyRetain,
			)

			Expect(stale).To(BeEmpty())
		})
	})

	Context("When resolving connection details", func() {
		It("should build a management client from direct connection details", func() {
			host := "rabbitmq.default.svc.cluster.local"
			port := int32(5672)
			username := "admin"
			password := "secret"

			reconciler := &RabbitMQAccessReconciler{}
			rbq := &accessv1.RabbitMQAccess{
				Spec: accessv1.RabbitMQAccessSpec{
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Username: &accessv1.SecretKeySelector{Value: &username},
						Password: &accessv1.SecretKeySelector{Value: &password},
					},
				},
			}

			client, err := reconciler.initializeRabbitMQClientConnection(context.Background(), rbq)
			Expect(err).NotTo(HaveOccurred())
			Expect(client.Endpoint).To(Equal("http://rabbitmq.default.svc.cluster.local:15672"))
			Expect(client.Username).To(Equal(username))
			Expect(client.Password).To(Equal(password))
		})

		It("should build a management client from an existing secret", func() {
			secretName := "rabbitmq-admin"
			fakeClient, _ := newFakeClientWithScheme(
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secretName,
						Namespace: "default",
					},
					Data: map[string][]byte{
						"host":     []byte("rabbitmq"),
						"port":     []byte(strconv.Itoa(5672)),
						"username": []byte("admin"),
						"password": []byte("secret"),
					},
				},
			)

			reconciler := &RabbitMQAccessReconciler{Client: fakeClient}
			rbq := &accessv1.RabbitMQAccess{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: accessv1.RabbitMQAccessSpec{
					Connection: accessv1.ConnectionSpec{
						ExistingSecret: &secretName,
					},
				},
			}

			client, err := reconciler.initializeRabbitMQClientConnection(context.Background(), rbq)
			Expect(err).NotTo(HaveOccurred())
			Expect(client.Endpoint).To(Equal("http://rabbitmq.default.svc:15672"))
			Expect(client.Username).To(Equal("admin"))
			Expect(client.Password).To(Equal("secret"))
		})
	})

	Context("When resolving RabbitMQ connection usernames", func() {
		It("should preserve usernames declared in connection details", func() {
			host := "rabbitmq.default.svc.cluster.local"
			port := int32(5672)
			username := "admin"
			password := "secret"

			fakeClient, fakeScheme := newFakeClientWithScheme(
				&accessv1.RabbitMQAccess{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rabbitmq-access",
						Namespace: "default",
					},
					Spec: accessv1.RabbitMQAccessSpec{
						Username:        "app-user",
						GeneratedSecret: "app-user-rabbitmq",
						Connection: accessv1.ConnectionSpec{
							Host:     &host,
							Port:     &port,
							Username: &accessv1.SecretKeySelector{Value: &username},
							Password: &accessv1.SecretKeySelector{Value: &password},
						},
						Permissions: []accessv1.RabbitMQPermissionSpec{
							{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
						},
					},
				},
			)

			reconciler := &RabbitMQAccessReconciler{Client: fakeClient, Scheme: fakeScheme}
			connectionUsers, err := reconciler.getAllRabbitMQConnectionUsernames(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(connectionUsers).To(HaveKey("admin"))
			Expect(connectionUsers).NotTo(HaveKey("app-user"))
		})
	})

	Context("When reconciling generated credentials secrets", func() {
		It("should create a generated secret for RabbitMQAccess", func() {
			rbq := &accessv1.RabbitMQAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rabbitmq-access",
					Namespace: "default",
				},
				Spec: accessv1.RabbitMQAccessSpec{
					Username:        "app-user",
					GeneratedSecret: "app-user-rabbitmq",
					Permissions: []accessv1.RabbitMQPermissionSpec{
						{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
					},
				},
			}

			fakeClient, fakeScheme := newFakeClientWithScheme(rbq)
			reconciler := &RabbitMQAccessReconciler{Client: fakeClient, Scheme: fakeScheme}

			configs, err := reconciler.getAllRabbitMQUserConfigs(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(configs).To(HaveKey("app-user"))
			Expect(configs["app-user"].Password).NotTo(BeEmpty())

			secret := &corev1.Secret{}
			err = fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      "app-user-rabbitmq",
				Namespace: "default",
			}, secret)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(secret.Data["username"])).To(Equal("app-user"))
			Expect(string(secret.Data["password"])).To(Equal(configs["app-user"].Password))
		})

		It("should reuse the existing password when updating a generated secret", func() {
			rbq := &accessv1.RabbitMQAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rabbitmq-access",
					Namespace: "default",
				},
				Spec: accessv1.RabbitMQAccessSpec{
					Username:        "app-user",
					GeneratedSecret: "app-user-rabbitmq",
					Permissions: []accessv1.RabbitMQPermissionSpec{
						{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
					},
				},
			}

			existingSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "app-user-rabbitmq",
					Namespace: "default",
				},
				Data: map[string][]byte{
					"username": []byte("stale-user"),
					"password": []byte("kept-password"),
				},
			}

			fakeClient, fakeScheme := newFakeClientWithScheme(rbq, existingSecret)
			reconciler := &RabbitMQAccessReconciler{Client: fakeClient, Scheme: fakeScheme}

			configs, err := reconciler.getAllRabbitMQUserConfigs(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(configs["app-user"].Password).To(Equal("kept-password"))

			updatedSecret := &corev1.Secret{}
			err = fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      "app-user-rabbitmq",
				Namespace: "default",
			}, updatedSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(updatedSecret.Data["username"])).To(Equal("app-user"))
			Expect(string(updatedSecret.Data["password"])).To(Equal("kept-password"))
		})
	})
})
