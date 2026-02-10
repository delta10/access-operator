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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

var _ = Describe("PostgresAccess Controller", func() {
	Context("When testing PostgreSQL connections", func() {
		const connectionTestResourceName = "postgres-connection-test"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      connectionTestResourceName,
			Namespace: "default",
		}

		AfterEach(func() {
			resource := &accessv1.PostgresAccess{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				By("Cleanup the postgres connection test resource")
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
		})

		It("should create secret and user with a connection and username specified", func() {
			By("creating a PostgresAccess resource with valid connection host and a user in postgres")
			host := "localhost"
			port := int32(5432)
			db := "testdb"
			username := "demo-user"
			password := "demo-password"
			resource := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      connectionTestResourceName,
					Namespace: "default",
				},
				Spec: accessv1.PostgresAccessSpec{
					GeneratedSecret: "test-connection-secret",
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Database: &db,
						Username: &accessv1.SecretKeySelector{
							Value: &username,
						},
						Password: &accessv1.SecretKeySelector{
							Value: &password,
						},
					},
					Username: &username,
					Grants: []accessv1.GrantSpec{
						{
							Database:   "testdb",
							Privileges: []string{"CONNECT", "SELECT", "INSERT"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			By("verifying the resource was created")
			createdResource := &accessv1.PostgresAccess{}
			err := k8sClient.Get(ctx, typeNamespacedName, createdResource)
			Expect(err).NotTo(HaveOccurred())
			Expect(createdResource.Spec.Connection.Host).NotTo(BeNil())
			Expect(*createdResource.Spec.Connection.Host).To(Equal("localhost"))

			By("creating a mock database")
			mockDB := NewMockDB()

			By("reconciling the resource")
			controllerReconciler := &PostgresAccessReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				DB:     mockDB,
			}

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("verifying the mock database was called correctly")
			Expect(mockDB.ConnectCalled).To(BeTrue())
			Expect(mockDB.LastConnectionString).To(ContainSubstring("demo-user:demo-password@localhost:5432/testdb"))
			Expect(mockDB.CreateUserCalled).To(BeTrue())
			Expect(mockDB.LastUsername).To(Equal("demo-user"))
			Expect(mockDB.GrantPrivilegesCalled).To(BeTrue())
			Expect(mockDB.LastGrants).To(HaveLen(1))

			By("verifying the secret was created with the specified username and password")
			secret := &corev1.Secret{}
			secretKey := types.NamespacedName{
				Name:      "test-connection-secret",
				Namespace: "default",
			}
			err = k8sClient.Get(ctx, secretKey, secret)
			Expect(err).NotTo(HaveOccurred())
			Expect(secret.Data).To(HaveKey("username"))
			Expect(secret.Data).To(HaveKey("password"))
			Expect(string(secret.Data["username"])).To(Equal("demo-user"))
			Expect(string(secret.Data["password"])).NotTo(BeEmpty())

			initialPassword := string(secret.Data["password"])

			By("reconciling again and verifying credentials remain stable")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			updatedSecret := &corev1.Secret{}
			err = k8sClient.Get(ctx, secretKey, updatedSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(updatedSecret.Data["password"])).To(Equal(initialPassword))
			Expect(mockDB.LastPassword).To(Equal(initialPassword))
		})
	})
})
