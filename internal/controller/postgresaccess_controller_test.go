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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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
				if controllerutil.ContainsFinalizer(resource, postgresAccessFinalizer) {
					controllerutil.RemoveFinalizer(resource, postgresAccessFinalizer)
					Expect(k8sClient.Update(ctx, resource)).To(Succeed())
				}
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
				Eventually(func() bool {
					err := k8sClient.Get(ctx, typeNamespacedName, &accessv1.PostgresAccess{})
					return apierrors.IsNotFound(err)
				}).Should(BeTrue())
			}

			By("Cleanup test secrets")
			_ = k8sClient.Delete(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-connection-secret",
					Namespace: "default",
				},
			})
			_ = k8sClient.Delete(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "db-credentials",
					Namespace: "default",
				},
			})
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
					Username: username,
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

			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(privilegeDriftRequeueInterval))

			By("verifying the mock database was called correctly")
			Expect(mockDB.ConnectCalled).To(BeTrue())
			Expect(mockDB.LastConnectionString).To(ContainSubstring("demo-user:demo-password@localhost:5432/testdb"))
			Expect(mockDB.CreateUserCalled).To(BeTrue())
			Expect(mockDB.LastCreatedUsername).To(Equal("demo-user"))
			Expect(mockDB.GrantPrivilegesCalled).To(BeTrue())

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
		})

		It("should create secret and user with a connection and username specified in an existing secret", func() {
			By("creating a PostgresAccess resource with valid connection host and a user in postgres")
			host := "localhost"
			port := int32(5432)
			db := "testdb"
			username := "demo-user"
			password := "demo-password"
			secretName := "db-credentials"

			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: "default",
				},
				Data: map[string][]byte{
					"username": []byte(username),
					"password": []byte(password),
					"host":     []byte(host),
					"port":     []byte(string(port)),
					"database": []byte(db),
				},
			}
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())

			resource := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      connectionTestResourceName,
					Namespace: "default",
				},
				Spec: accessv1.PostgresAccessSpec{
					GeneratedSecret: "test-connection-secret",
					Connection: accessv1.ConnectionSpec{
						ExistingSecret: &secretName,
					},
					Username: username,
					Grants: []accessv1.GrantSpec{
						{
							Database:   "testdb",
							Privileges: []string{"CONNECT", "SELECT", "INSERT"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			By("reconciling the resource")
			controllerReconciler := &PostgresAccessReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				DB:     NewMockDB(),
			}

			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(privilegeDriftRequeueInterval))

			By("verifying the secret was created with the specified username and password")
			createdSecret := &corev1.Secret{}
			secretKey := types.NamespacedName{
				Name:      "test-connection-secret",
				Namespace: "default",
			}
			err = k8sClient.Get(ctx, secretKey, createdSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(createdSecret.Data).To(HaveKey("username"))
			Expect(string(createdSecret.Data["username"])).To(Equal(username))
		})
	})

	Context("When testing helper logic with fake clients", func() {
		It("should default ssl mode to require for direct connection details", func() {
			username := "db-admin"
			password := "secret"
			host := "postgres.default.svc"
			port := int32(5432)
			database := "appdb"

			reconciler := &PostgresAccessReconciler{}
			pg := &accessv1.PostgresAccess{
				Spec: accessv1.PostgresAccessSpec{
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Database: &database,
						Username: &accessv1.SecretKeySelector{Value: &username},
						Password: &accessv1.SecretKeySelector{Value: &password},
					},
				},
			}

			connectionString, err := reconciler.getConnectionString(context.Background(), pg)
			Expect(err).NotTo(HaveOccurred())
			Expect(connectionString).To(Equal("postgresql://db-admin:secret@postgres.default.svc:5432/appdb?sslmode=require"))
		})

		It("should default ssl mode to require when missing in existing secret", func() {
			fakeClient, _ := newFakeClientWithScheme(
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "db-connection",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"host":     []byte("postgres.default.svc"),
						"port":     []byte("5432"),
						"database": []byte("appdb"),
						"username": []byte("db-admin"),
						"password": []byte("secret"),
					},
				},
			)

			connectionDetails, err := getExistingSecretConnectionDetails(context.Background(), fakeClient, "db-connection", "default")
			Expect(err).NotTo(HaveOccurred())
			Expect(connectionDetails.SSLMode).To(Equal("require"))
		})

		It("should use an optional explicitly configured ssl mode for direct connection details", func() {
			username := "db-admin"
			password := "secret"
			host := "postgres.default.svc"
			port := int32(5432)
			database := "appdb"
			sslMode := "disable"

			reconciler := &PostgresAccessReconciler{}
			pg := &accessv1.PostgresAccess{
				Spec: accessv1.PostgresAccessSpec{
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Database: &database,
						SSLMode:  &sslMode,
						Username: &accessv1.SecretKeySelector{Value: &username},
						Password: &accessv1.SecretKeySelector{Value: &password},
					},
				},
			}

			connectionString, err := reconciler.getConnectionString(context.Background(), pg)
			Expect(err).NotTo(HaveOccurred())
			Expect(connectionString).To(Equal("postgresql://db-admin:secret@postgres.default.svc:5432/appdb?sslmode=disable"))
		})

		It("should build connection strings from an existing secret", func() {
			fakeClient, _ := newFakeClientWithScheme(
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "db-connection",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"host":     []byte("postgres.default.svc"),
						"port":     []byte("5432"),
						"database": []byte("appdb"),
						"username": []byte("db-admin"),
						"password": []byte("secret"),
					},
				},
			)

			reconciler := &PostgresAccessReconciler{Client: fakeClient}
			secretName := "db-connection"
			pg := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: accessv1.PostgresAccessSpec{
					Connection: accessv1.ConnectionSpec{
						ExistingSecret: &secretName,
					},
				},
			}

			connectionString, err := reconciler.getConnectionString(context.Background(), pg)
			Expect(err).NotTo(HaveOccurred())
			Expect(connectionString).To(Equal("postgresql://db-admin:secret@postgres.default.svc:5432/appdb?sslmode=require"))
		})

		It("should return an error when no valid connection details are provided", func() {
			reconciler := &PostgresAccessReconciler{}
			pg := &accessv1.PostgresAccess{
				Spec: accessv1.PostgresAccessSpec{
					Connection: accessv1.ConnectionSpec{},
				},
			}

			_, err := reconciler.getConnectionString(context.Background(), pg)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no valid connection details provided"))
		})

		DescribeTable(
			"secret validation",
			func(missingKey string, expectedError string) {
				data := map[string][]byte{
					"host":     []byte("postgres.default.svc"),
					"port":     []byte("5432"),
					"database": []byte("appdb"),
					"username": []byte("db-admin"),
					"password": []byte("secret"),
				}
				delete(data, missingKey)

				fakeClient, _ := newFakeClientWithScheme(
					&corev1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "db-connection",
							Namespace: "default",
						},
						Data: data,
					},
				)

				_, err := getExistingSecretConnectionDetails(context.Background(), fakeClient, "db-connection", "default")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(expectedError))
			},
			Entry("missing username", "username", "missing username"),
			Entry("missing password", "password", "missing password"),
			Entry("missing host", "host", "missing host"),
			Entry("missing port", "port", "missing port"),
			Entry("missing database", "database", "missing database"),
		)

		It("should compute grant and revoke sets for changed privileges", func() {
			current := []accessv1.GrantSpec{
				{Database: "appdb", Privileges: []string{"CONNECT", "SELECT"}},
			}
			desired := []accessv1.GrantSpec{
				{Database: "appdb", Privileges: []string{"CONNECT", "INSERT"}},
			}

			toGrant, toRevoke := diffGrants(current, desired)

			grantSet := grantKeySet(toGrant)
			revokeSet := grantKeySet(toRevoke)

			Expect(grantSet).To(HaveLen(1))
			Expect(grantSet).To(HaveKey("appdb:INSERT"))

			Expect(revokeSet).To(HaveLen(1))
			Expect(revokeSet).To(HaveKey("appdb:SELECT"))
		})

		It("should grant all desired privileges when there are no current privileges", func() {
			desired := []accessv1.GrantSpec{
				{Database: "appdb", Privileges: []string{"CONNECT", "SELECT"}},
			}

			toGrant, toRevoke := diffGrants(nil, desired)
			grantSet := grantKeySet(toGrant)

			Expect(toRevoke).To(BeEmpty())
			Expect(grantSet).To(HaveLen(2))
			Expect(grantSet).To(HaveKey("appdb:CONNECT"))
			Expect(grantSet).To(HaveKey("appdb:SELECT"))
		})

		It("should list PostgresAccess users and grants only from the target namespace", func() {
			firstUser := "app-1"
			secondUser := "app-2"
			otherNamespaceUser := "other"

			fakeClient, _ := newFakeClientWithScheme(
				&accessv1.PostgresAccess{
					ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "target"},
					Spec: accessv1.PostgresAccessSpec{
						GeneratedSecret: "first-secret",
						Username:        firstUser,
						Grants: []accessv1.GrantSpec{
							{Database: "db1", Privileges: []string{"CONNECT"}},
						},
					},
				},
				&accessv1.PostgresAccess{
					ObjectMeta: metav1.ObjectMeta{Name: "second", Namespace: "target"},
					Spec: accessv1.PostgresAccessSpec{
						GeneratedSecret: "second-secret",
						Username:        secondUser,
						Grants: []accessv1.GrantSpec{
							{Database: "db2", Privileges: []string{"SELECT"}},
						},
					},
				},
				&accessv1.PostgresAccess{
					ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "other-ns"},
					Spec: accessv1.PostgresAccessSpec{
						GeneratedSecret: "other-secret",
						Username:        otherNamespaceUser,
						Grants: []accessv1.GrantSpec{
							{Database: "db3", Privileges: []string{"CONNECT"}},
						},
					},
				},
			)

			results, err := getAllPostgresAccessGrantsAndUsers(context.Background(), fakeClient, "target")
			Expect(err).NotTo(HaveOccurred())
			Expect(results).To(HaveLen(2))

			resultMap := make(map[string]UserGrants, len(results))
			for _, item := range results {
				resultMap[item.Username] = item
			}

			Expect(resultMap).To(HaveKey(firstUser))
			Expect(resultMap).To(HaveKey(secondUser))
			Expect(resultMap).NotTo(HaveKey(otherNamespaceUser))
			Expect(resultMap[firstUser].GeneratedSecret).To(Equal("first-secret"))
			Expect(resultMap[secondUser].GeneratedSecret).To(Equal("second-secret"))
		})

		It("should read user password from the generated secret name", func() {
			fakeClient, _ := newFakeClientWithScheme(
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "custom-generated-secret",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"password": []byte("super-secret"),
					},
				},
			)

			password, err := getUserPassword(context.Background(), fakeClient, "default", "custom-generated-secret")
			Expect(err).NotTo(HaveOccurred())
			Expect(password).To(Equal("super-secret"))
		})

		It("should add finalizer for active resources", func() {
			pg := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "finalizer-test",
					Namespace: "default",
				},
			}
			fakeClient, fakeScheme := newFakeClientWithScheme(pg)

			reconciler := &PostgresAccessReconciler{
				Client: fakeClient,
				Scheme: fakeScheme,
			}

			finalized, err := reconciler.finalizePostgresAccess(context.Background(), pg)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeFalse())

			updated := &accessv1.PostgresAccess{}
			err = fakeClient.Get(context.Background(), client.ObjectKey{Name: "finalizer-test", Namespace: "default"}, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.ContainsFinalizer(updated, postgresAccessFinalizer)).To(BeTrue())
		})

		It("should return finalized for deleting resources without finalizer", func() {
			now := metav1.NewTime(time.Now())
			pg := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "finalizer-test",
					Namespace:         "default",
					DeletionTimestamp: &now,
				},
			}

			reconciler := &PostgresAccessReconciler{}
			finalized, err := reconciler.finalizePostgresAccess(context.Background(), pg)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
		})
	})
})

func grantKeySet(grants []accessv1.GrantSpec) map[string]struct{} {
	out := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		for _, privilege := range grant.Privileges {
			out[grant.Database+":"+privilege] = struct{}{}
		}
	}
	return out
}

func newFakeClientWithScheme(objs ...client.Object) (client.Client, *runtime.Scheme) {
	scheme := runtime.NewScheme()
	Expect(accessv1.AddToScheme(scheme)).To(Succeed())
	Expect(corev1.AddToScheme(scheme)).To(Succeed())

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()

	return fakeClient, scheme
}
