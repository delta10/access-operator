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
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

var _ = Describe("RabbitMQAccess Controller", func() {
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

	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		rabbitmqaccess := &accessv1.RabbitMQAccess{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind RabbitMQAccess")
			err := k8sClient.Get(ctx, typeNamespacedName, rabbitmqaccess)
			if err != nil && errors.IsNotFound(err) {
				resource := &accessv1.RabbitMQAccess{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					// TODO(user): Specify other spec details if needed.
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &accessv1.RabbitMQAccess{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance RabbitMQAccess")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
	})
})
