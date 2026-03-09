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
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

const localHost = "localhost"

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

		host := localHost
		port := int32(5432)
		db := "testdb"
		username := "demo-user"
		password := "demo-password"

		It("should create secret and user with a connection and username specified", func() {
			By("creating a PostgresAccess resource with valid connection host and a user in postgres")
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
			Expect(*createdResource.Spec.Connection.Host).To(Equal(localHost))

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
					"port":     []byte(strconv.Itoa(int(port))),
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
		var username = "db-admin"
		var password = "secret"
		var host = "postgres.default.svc"
		var port = int32(5432)
		var database = "appdb"
		var secretName = "db-connection"
		var sharedSecretNamespace = "shared-db"

		It("should default ssl mode to require for direct connection details", func() {
			expectedString := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=require", username, password, host, port, database)

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
			Expect(connectionString).To(Equal(expectedString))
		})

		It("should default ssl mode to require when missing in existing secret", func() {
			fakeClient, _ := newFakeClientWithScheme(
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secretName,
						Namespace: "default",
					},
					Data: map[string][]byte{
						"host":     []byte(host),
						"port":     []byte(strconv.Itoa(int(port))),
						"database": []byte(database),
						"username": []byte(username),
						"password": []byte(password),
					},
				},
			)

			connectionDetails, err := getExistingSecretConnectionDetails(context.Background(), fakeClient, secretName, "default", nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(connectionDetails.SSLMode).To(Equal("require"))
		})

		It("should use an optional explicitly configured ssl mode for direct connection details", func() {
			sslMode := "disable"
			expectedString := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=%s", username, password, host, port, database, sslMode)

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
			Expect(connectionString).To(Equal(expectedString))
		})

		It("should build connection strings from an existing secret", func() {
			expectedString := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=require", username, password, host, port, database)
			fakeClient, _ := newFakeClientWithScheme(
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secretName,
						Namespace: "default",
					},
					Data: map[string][]byte{
						"host":     []byte(host),
						"port":     []byte(strconv.Itoa(int(port))),
						"database": []byte(database),
						"username": []byte(username),
						"password": []byte(password),
					},
				},
			)

			reconciler := &PostgresAccessReconciler{Client: fakeClient}
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
			Expect(connectionString).To(Equal(expectedString))
		})

		It("should reject cross-namespace existingSecret when no Controller resource exists", func() {
			fakeClient, _ := newFakeClientWithScheme(
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secretName,
						Namespace: sharedSecretNamespace,
					},
					Data: map[string][]byte{
						"host":     []byte("postgres"),
						"port":     []byte(strconv.Itoa(int(port))),
						"database": []byte(database),
						"username": []byte(username),
						"password": []byte(password),
					},
				},
			)

			reconciler := &PostgresAccessReconciler{Client: fakeClient}
			secretNamespace := sharedSecretNamespace
			pg := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a"},
				Spec: accessv1.PostgresAccessSpec{
					Connection: accessv1.ConnectionSpec{
						ExistingSecret:          &secretName,
						ExistingSecretNamespace: &secretNamespace,
					},
				},
			}

			_, err := reconciler.getConnectionString(context.Background(), pg)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cross-namespace connection secret references are disabled"))
		})

		It("should reject cross-namespace existingSecret when singleton Controller policy is false", func() {
			fakeClient, _ := newFakeClientWithScheme(
				&accessv1.Controller{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cluster-settings",
						Namespace: "system",
					},
					Spec: accessv1.ControllerSpec{
						Settings: accessv1.ControllerSettings{
							ExistingSecretNamespace: false,
						},
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secretName,
						Namespace: sharedSecretNamespace,
					},
					Data: map[string][]byte{
						"host":     []byte("postgres"),
						"port":     []byte(strconv.Itoa(int(port))),
						"database": []byte(database),
						"username": []byte(username),
						"password": []byte(password),
					},
				},
			)

			reconciler := &PostgresAccessReconciler{Client: fakeClient}
			secretNamespace := sharedSecretNamespace
			pg := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a"},
				Spec: accessv1.PostgresAccessSpec{
					Connection: accessv1.ConnectionSpec{
						ExistingSecret:          &secretName,
						ExistingSecretNamespace: &secretNamespace,
					},
				},
			}

			_, err := reconciler.getConnectionString(context.Background(), pg)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cross-namespace connection secret references are disabled"))
		})

		It("should allow cross-namespace existingSecret when singleton Controller policy is true", func() {
			fakeClient, _ := newFakeClientWithScheme(
				&accessv1.Controller{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cluster-settings",
						Namespace: "system",
					},
					Spec: accessv1.ControllerSpec{
						Settings: accessv1.ControllerSettings{
							ExistingSecretNamespace: true,
						},
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secretName,
						Namespace: sharedSecretNamespace,
					},
					Data: map[string][]byte{
						"host":     []byte("postgres"),
						"port":     []byte(strconv.Itoa(int(port))),
						"database": []byte(database),
						"username": []byte(username),
						"password": []byte(password),
					},
				},
			)

			reconciler := &PostgresAccessReconciler{Client: fakeClient}
			secretNamespace := sharedSecretNamespace
			pg := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a"},
				Spec: accessv1.PostgresAccessSpec{
					Connection: accessv1.ConnectionSpec{
						ExistingSecret:          &secretName,
						ExistingSecretNamespace: &secretNamespace,
					},
				},
			}

			connectionString, err := reconciler.getConnectionString(context.Background(), pg)
			Expect(err).NotTo(HaveOccurred())
			Expect(connectionString).To(Equal("postgresql://db-admin:secret@postgres.shared-db.svc:5432/appdb?sslmode=require"))
		})

		It("should normalize excluded usernames from singleton Controller settings", func() {
			fakeClient, _ := newFakeClientWithScheme(
				&accessv1.Controller{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cluster-settings",
						Namespace: "system",
					},
					Spec: accessv1.ControllerSpec{
						Settings: accessv1.ControllerSettings{
							PostgresSettings: accessv1.PostgresControllerSettings{
								ExcludedUsers: []string{" postgres ", "", "app-user", "postgres"},
							},
						},
					},
				},
			)

			reconciler := &PostgresAccessReconciler{Client: fakeClient}
			excludedUsers, err := reconciler.resolveExcludedUsers(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(excludedUsers).To(HaveLen(2))
			Expect(excludedUsers).To(HaveKey("postgres"))
			Expect(excludedUsers).To(HaveKey("app-user"))
		})

		It("should hard fail cross-namespace existingSecret when multiple Controller resources exist", func() {
			fakeClient, _ := newFakeClientWithScheme(
				&accessv1.Controller{
					ObjectMeta: metav1.ObjectMeta{Name: "controller-a", Namespace: "system"},
					Spec: accessv1.ControllerSpec{
						Settings: accessv1.ControllerSettings{ExistingSecretNamespace: true},
					},
				},
				&accessv1.Controller{
					ObjectMeta: metav1.ObjectMeta{Name: "controller-b", Namespace: "default"},
					Spec: accessv1.ControllerSpec{
						Settings: accessv1.ControllerSettings{ExistingSecretNamespace: true},
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secretName,
						Namespace: "shared-db",
					},
					Data: map[string][]byte{
						"host":     []byte("postgres"),
						"port":     []byte(strconv.Itoa(int(port))),
						"database": []byte(database),
						"username": []byte(username),
						"password": []byte(password),
					},
				},
			)

			eventRecorder := events.NewFakeRecorder(10)
			reconciler := &PostgresAccessReconciler{
				Client:   fakeClient,
				Recorder: eventRecorder,
			}
			secretNamespace := "shared-db"
			pg := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{Name: "tenant-access", Namespace: "tenant-a"},
				Spec: accessv1.PostgresAccessSpec{
					Connection: accessv1.ConnectionSpec{
						ExistingSecret:          &secretName,
						ExistingSecretNamespace: &secretNamespace,
					},
				},
			}

			_, err := reconciler.getConnectionString(context.Background(), pg)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("multiple Controller resources found"))

			var eventOne, eventTwo, eventThree string
			Eventually(eventRecorder.Events).Should(Receive(&eventOne))
			Eventually(eventRecorder.Events).Should(Receive(&eventTwo))
			Eventually(eventRecorder.Events).Should(Receive(&eventThree))

			allEvents := strings.Join([]string{eventOne, eventTwo, eventThree}, " ")
			Expect(allEvents).To(ContainSubstring(multipleControllersFoundReason))
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
					"host":     []byte(host),
					"port":     []byte(strconv.Itoa(int(port))),
					"database": []byte(database),
					"username": []byte(username),
					"password": []byte(password),
				}
				delete(data, missingKey)

				fakeClient, _ := newFakeClientWithScheme(
					&corev1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      secretName,
							Namespace: "default",
						},
						Data: data,
					},
				)

				_, err := getExistingSecretConnectionDetails(context.Background(), fakeClient, secretName, "default", nil)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(expectedError))
			},
			Entry("missing username", "username", "missing username"),
			Entry("missing password", "password", "missing password"),
			Entry("missing host", "host", "missing host"),
			Entry("missing port", "port", "missing port"),
		)

		It("should fall back to postgres database when database name is missing or invalid in existing secret", func() {
			expectedString := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=require", username, password, host, port, "postgres")
			fakeClient, _ := newFakeClientWithScheme(
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secretName,
						Namespace: "default",
					},
					Data: map[string][]byte{
						"host":     []byte(host),
						"port":     []byte(strconv.Itoa(int(port))),
						"database": []byte("*"), // invalid database name
						"username": []byte(username),
						"password": []byte(password),
					},
				},
			)

			reconciler := &PostgresAccessReconciler{Client: fakeClient}

			connectionString, err := reconciler.getConnectionString(context.Background(), &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: accessv1.PostgresAccessSpec{
					Connection: accessv1.ConnectionSpec{
						ExistingSecret: &secretName,
					},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(connectionString).To(Equal(expectedString))
		})

		It("should compute grant and revoke sets for changed privileges", func() {
			current := []accessv1.GrantSpec{
				{Database: database, Privileges: []string{"CONNECT", "SELECT"}},
			}
			desired := []accessv1.GrantSpec{
				{Database: database, Privileges: []string{"CONNECT", "INSERT"}},
			}

			toGrant, toRevoke := diffGrants(current, desired)

			grantSet := grantKeySet(toGrant)
			revokeSet := grantKeySet(toRevoke)

			Expect(grantSet).To(HaveLen(1))
			Expect(grantSet).To(HaveKey("appdb:public:INSERT"))

			Expect(revokeSet).To(HaveLen(1))
			Expect(revokeSet).To(HaveKey("appdb:public:SELECT"))
		})

		It("should grant all desired privileges when there are no current privileges", func() {
			desired := []accessv1.GrantSpec{
				{Database: database, Privileges: []string{"CONNECT", "SELECT"}},
			}

			toGrant, toRevoke := diffGrants(nil, desired)
			grantSet := grantKeySet(toGrant)

			Expect(toRevoke).To(BeEmpty())
			Expect(grantSet).To(HaveLen(2))
			Expect(grantSet).To(HaveKey("appdb:public:CONNECT"))
			Expect(grantSet).To(HaveKey("appdb:public:SELECT"))
		})

		It("should treat grants in different schemas as distinct", func() {
			publicSchema := defaultSchemaName
			accountingSchema := "accounting"
			current := []accessv1.GrantSpec{
				{Database: database, Schema: &publicSchema, Privileges: []string{"SELECT"}},
			}
			desired := []accessv1.GrantSpec{
				{Database: database, Schema: &accountingSchema, Privileges: []string{"SELECT"}},
			}

			toGrant, toRevoke := diffGrants(current, desired)

			grantSet := grantKeySet(toGrant)
			revokeSet := grantKeySet(toRevoke)

			Expect(grantSet).To(HaveLen(1))
			Expect(grantSet).To(HaveKey("appdb:accounting:SELECT"))
			Expect(revokeSet).To(HaveLen(1))
			Expect(revokeSet).To(HaveKey("appdb:public:SELECT"))
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

		It("should set Ready=False when reconcile returns an error", func() {
			pg := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "status-error",
					Namespace: "default",
				},
				Spec: accessv1.PostgresAccessSpec{
					GeneratedSecret: "status-error-secret",
					Username:        "demo-user",
					Connection:      accessv1.ConnectionSpec{},
					Grants: []accessv1.GrantSpec{
						{Database: "appdb", Privileges: []string{"CONNECT"}},
					},
				},
			}

			fakeClient, fakeScheme := newFakeClientWithScheme(pg)
			eventRecorder := events.NewFakeRecorder(5)
			reconciler := &PostgresAccessReconciler{
				Client:   fakeClient,
				Scheme:   fakeScheme,
				DB:       NewMockDB(),
				Recorder: eventRecorder,
			}

			_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(pg),
			})
			Expect(err).To(HaveOccurred())

			updated := &accessv1.PostgresAccess{}
			err = fakeClient.Get(context.Background(), client.ObjectKeyFromObject(pg), updated)
			Expect(err).NotTo(HaveOccurred())

			readyCondition := meta.FindStatusCondition(updated.Status.Conditions, postgresAccessReadyConditionType)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCondition.Reason).To(Equal("DatabaseSyncFailed"))

			successCondition := meta.FindStatusCondition(updated.Status.Conditions, postgresAccessSuccessConditionType)
			Expect(successCondition).NotTo(BeNil())
			Expect(successCondition.Status).To(Equal(metav1.ConditionFalse))

			inProgressCondition := meta.FindStatusCondition(updated.Status.Conditions, postgresAccessInProgressConditionType)
			Expect(inProgressCondition).NotTo(BeNil())
			Expect(inProgressCondition.Status).To(Equal(metav1.ConditionFalse))

			Expect(updated.Status.LastReconcileState).To(Equal(accessv1.ReconcileStateError))
			Expect(updated.Status.LastLog).To(ContainSubstring("no valid connection details provided"))

			var event string
			Eventually(eventRecorder.Events).Should(Receive(&event))
			Expect(event).To(ContainSubstring("DatabaseSyncFailed"))
		})

		It("should set Ready=True when reconcile succeeds in syncing state", func() {
			host := localHost
			port := int32(5432)
			username := "user1"

			pg := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "status-ready",
					Namespace: "default",
				},
				Spec: accessv1.PostgresAccessSpec{
					GeneratedSecret: "status-ready-secret",
					Username:        username,
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Database: &database,
						Username: &accessv1.SecretKeySelector{Value: &username},
						Password: &accessv1.SecretKeySelector{Value: &password},
					},
				},
			}

			fakeClient, fakeScheme := newFakeClientWithScheme(pg)
			reconciler := &PostgresAccessReconciler{
				Client: fakeClient,
				Scheme: fakeScheme,
				DB:     NewMockDB(),
			}

			result, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(pg),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(privilegeDriftRequeueInterval))

			inProgressStatus := &accessv1.PostgresAccess{}
			err = fakeClient.Get(context.Background(), client.ObjectKeyFromObject(pg), inProgressStatus)
			Expect(err).NotTo(HaveOccurred())

			readyCondition := meta.FindStatusCondition(inProgressStatus.Status.Conditions, postgresAccessReadyConditionType)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCondition.Reason).To(Equal("Reconciling"))

			successCondition := meta.FindStatusCondition(inProgressStatus.Status.Conditions, postgresAccessSuccessConditionType)
			Expect(successCondition).NotTo(BeNil())
			Expect(successCondition.Status).To(Equal(metav1.ConditionFalse))

			inProgressCondition := meta.FindStatusCondition(inProgressStatus.Status.Conditions, postgresAccessInProgressConditionType)
			Expect(inProgressCondition).NotTo(BeNil())
			Expect(inProgressCondition.Status).To(Equal(metav1.ConditionTrue))

			Expect(inProgressStatus.Status.LastReconcileState).To(Equal(accessv1.ReconcileStateInProgress))
			Expect(inProgressStatus.Status.LastLog).To(Equal("PostgresAccess is not yet in sync"))

			result, err = reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(pg),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(syncedRequeueInterval))

			updated := &accessv1.PostgresAccess{}
			err = fakeClient.Get(context.Background(), client.ObjectKeyFromObject(pg), updated)
			Expect(err).NotTo(HaveOccurred())

			readyCondition = meta.FindStatusCondition(updated.Status.Conditions, postgresAccessReadyConditionType)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCondition.Reason).To(Equal("Ready"))

			successCondition = meta.FindStatusCondition(updated.Status.Conditions, postgresAccessSuccessConditionType)
			Expect(successCondition).NotTo(BeNil())
			Expect(successCondition.Status).To(Equal(metav1.ConditionTrue))

			inProgressCondition = meta.FindStatusCondition(updated.Status.Conditions, postgresAccessInProgressConditionType)
			Expect(inProgressCondition).NotTo(BeNil())
			Expect(inProgressCondition.Status).To(Equal(metav1.ConditionFalse))

			Expect(updated.Status.LastReconcileState).To(Equal(accessv1.ReconcileStateSuccess))
			Expect(updated.Status.LastLog).To(Equal("PostgresAccess is in sync"))
		})

		It("should report success after applying missing grants for an existing user", func() {
			host := localHost
			port := int32(5432)
			username := "user1"

			pg := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "status-grants",
					Namespace: "default",
				},
				Spec: accessv1.PostgresAccessSpec{
					GeneratedSecret: "status-grants-secret",
					Username:        username,
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Database: &database,
						Username: &accessv1.SecretKeySelector{Value: &username},
						Password: &accessv1.SecretKeySelector{Value: &password},
					},
					Grants: []accessv1.GrantSpec{
						{Database: database, Privileges: []string{"CONNECT", "SELECT"}},
					},
				},
			}

			fakeClient, fakeScheme := newFakeClientWithScheme(
				pg,
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "status-grants-secret",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"username": []byte(username),
						"password": []byte(password),
					},
				},
			)
			mockDB := NewMockDB()
			mockDB.Users = []string{username}
			reconciler := &PostgresAccessReconciler{
				Client: fakeClient,
				Scheme: fakeScheme,
				DB:     mockDB,
			}

			result, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(pg),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(syncedRequeueInterval))
			Expect(mockDB.GrantPrivilegesCalled).To(BeTrue())
			Expect(grantKeySet(mockDB.LastGrants)).To(HaveKey("appdb:public:CONNECT"))
			Expect(grantKeySet(mockDB.LastGrants)).To(HaveKey("appdb:public:SELECT"))

			updated := &accessv1.PostgresAccess{}
			err = fakeClient.Get(context.Background(), client.ObjectKeyFromObject(pg), updated)
			Expect(err).NotTo(HaveOccurred())

			readyCondition := meta.FindStatusCondition(updated.Status.Conditions, postgresAccessReadyConditionType)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))
			Expect(updated.Status.LastReconcileState).To(Equal(accessv1.ReconcileStateSuccess))
		})

		It("should skip excluded users during reconciliation and finalization", func() {
			host := localHost
			port := int32(5432)
			username := "excluded-user"

			pg := &accessv1.PostgresAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "excluded-user",
					Namespace: "default",
				},
				Spec: accessv1.PostgresAccessSpec{
					GeneratedSecret: "excluded-user-secret",
					Username:        username,
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Database: &database,
						Username: &accessv1.SecretKeySelector{Value: &username},
						Password: &accessv1.SecretKeySelector{Value: &password},
					},
					Grants: []accessv1.GrantSpec{
						{Database: database, Privileges: []string{"CONNECT", "SELECT"}},
					},
				},
			}

			controllerSettings := &accessv1.Controller{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cluster-settings",
					Namespace: "system",
				},
				Spec: accessv1.ControllerSpec{
					Settings: accessv1.ControllerSettings{
						PostgresSettings: accessv1.PostgresControllerSettings{
							ExcludedUsers: []string{username, "excluded-orphan"},
						},
					},
				},
			}

			fakeClient, fakeScheme := newFakeClientWithScheme(pg, controllerSettings)
			mockDB := NewMockDB()
			mockDB.Users = []string{"excluded-orphan"}
			reconciler := &PostgresAccessReconciler{
				Client: fakeClient,
				Scheme: fakeScheme,
				DB:     mockDB,
			}

			result, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(pg),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(privilegeDriftRequeueInterval))
			Expect(mockDB.CreateUserCalled).To(BeFalse())
			Expect(mockDB.GrantPrivilegesCalled).To(BeFalse())
			Expect(mockDB.DropUserCalled).To(BeFalse())

			result, err = reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(pg),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(syncedRequeueInterval))

			now := metav1.NewTime(time.Now())
			deletingPG := pg.DeepCopy()
			deletingPG.Finalizers = []string{postgresAccessFinalizer}
			deletingPG.DeletionTimestamp = &now

			finalizerClient, finalizerScheme := newFakeClientWithScheme(deletingPG, controllerSettings.DeepCopy())
			finalizerReconciler := &PostgresAccessReconciler{
				Client: finalizerClient,
				Scheme: finalizerScheme,
				DB:     mockDB,
			}

			finalized, err := finalizerReconciler.finalizePostgresAccess(context.Background(), deletingPG)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
			Expect(mockDB.DropUserCalled).To(BeFalse())

			finalizedResource := &accessv1.PostgresAccess{}
			err = finalizerClient.Get(context.Background(), client.ObjectKeyFromObject(pg), finalizedResource)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})
})

func grantKeySet(grants []accessv1.GrantSpec) map[string]struct{} {
	out := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		for _, privilege := range grant.Privileges {
			schema := defaultSchemaName
			if grant.Schema != nil && *grant.Schema != "" {
				schema = *grant.Schema
			}
			out[grant.Database+":"+schema+":"+strings.ToUpper(privilege)] = struct{}{}
		}
	}
	return out
}

func newFakeClientWithScheme(objs ...client.Object) (client.Client, *runtime.Scheme) {
	scheme := runtime.NewScheme()
	Expect(accessv1.AddToScheme(scheme)).To(Succeed())
	Expect(corev1.AddToScheme(scheme)).To(Succeed())
	Expect(appsv1.AddToScheme(scheme)).To(Succeed())

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&accessv1.PostgresAccess{}, &accessv1.Controller{}).
		WithObjects(objs...).
		Build()

	return fakeClient, scheme
}
