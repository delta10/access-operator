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

package rabbitMQ

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/delta10/access-operator/internal/controller"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

var _ = Describe("RabbitMQAccess Controller", func() {
	const (
		testRabbitMQHost     = "rabbitmq.default.svc.cluster.local"
		testRabbitMQSecret   = "rabbitmq-admin"
		testRabbitMQUsername = "admin"
		testRabbitMQPassword = "secret"
	)

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

			reconciler := &AccessReconciler{Client: fakeClient}
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

			reconciler := &AccessReconciler{Client: fakeClient}
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
			desiredUsers := map[string]UserConfig{
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
			host := testRabbitMQHost
			port := int32(5672)
			username := testRabbitMQUsername
			password := testRabbitMQPassword

			reconciler := &AccessReconciler{}
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
			Expect(client.Username).To(Equal(testRabbitMQUsername))
			Expect(client.Password).To(Equal(testRabbitMQPassword))
		})

		It("should build a management client from an existing secret", func() {
			secretName := testRabbitMQSecret
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

			reconciler := &AccessReconciler{Client: fakeClient}
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

		It("should hard fail cross-namespace existingSecret when multiple Controller resources exist", func() {
			secretName := testRabbitMQSecret
			secretNamespace := "shared-rabbitmq"

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
						Namespace: secretNamespace,
					},
					Data: map[string][]byte{
						"host":     []byte("rabbitmq"),
						"port":     []byte(strconv.Itoa(5672)),
						"username": []byte("admin"),
						"password": []byte("secret"),
					},
				},
			)

			eventRecorder := events.NewFakeRecorder(10)
			reconciler := &AccessReconciler{
				Client:   fakeClient,
				Recorder: eventRecorder,
			}
			rbq := &accessv1.RabbitMQAccess{
				ObjectMeta: metav1.ObjectMeta{Name: "tenant-access", Namespace: "tenant-a"},
				Spec: accessv1.RabbitMQAccessSpec{
					Connection: accessv1.ConnectionSpec{
						ExistingSecret:          &secretName,
						ExistingSecretNamespace: &secretNamespace,
					},
				},
			}

			_, err := reconciler.getConnectionDetails(context.Background(), rbq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("multiple Controller resources found"))

			var eventOne, eventTwo, eventThree string
			Eventually(eventRecorder.Events).Should(Receive(&eventOne))
			Eventually(eventRecorder.Events).Should(Receive(&eventTwo))
			Eventually(eventRecorder.Events).Should(Receive(&eventThree))

			allEvents := strings.Join([]string{eventOne, eventTwo, eventThree}, " ")
			Expect(allEvents).To(ContainSubstring(controller.MultipleControllersFoundReason))
		})

		It("should reject cross-namespace existingSecret when singleton Controller is outside the operator namespace", func() {
			secretName := testRabbitMQSecret
			secretNamespace := "shared-rabbitmq"

			fakeClient, _ := newFakeClientWithScheme(
				&accessv1.Controller{
					ObjectMeta: metav1.ObjectMeta{Name: "cluster-settings", Namespace: "tenant-a"},
					Spec: accessv1.ControllerSpec{
						Settings: accessv1.ControllerSettings{ExistingSecretNamespace: true},
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secretName,
						Namespace: secretNamespace,
					},
					Data: map[string][]byte{
						"host":     []byte("rabbitmq"),
						"port":     []byte(strconv.Itoa(5672)),
						"username": []byte("admin"),
						"password": []byte("secret"),
					},
				},
			)

			reconciler := &AccessReconciler{Client: fakeClient}
			rbq := &accessv1.RabbitMQAccess{
				ObjectMeta: metav1.ObjectMeta{Name: "tenant-access", Namespace: "default"},
				Spec: accessv1.RabbitMQAccessSpec{
					Connection: accessv1.ConnectionSpec{
						ExistingSecret:          &secretName,
						ExistingSecretNamespace: &secretNamespace,
					},
				},
			}

			_, err := reconciler.getConnectionDetails(context.Background(), rbq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`must be created in the operator namespace "system"`))
		})
	})

	Context("When resolving RabbitMQ connection usernames", func() {
		It("should preserve usernames declared in connection details", func() {
			host := testRabbitMQHost
			port := int32(5672)
			username := testRabbitMQUsername
			password := testRabbitMQPassword

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

			reconciler := &AccessReconciler{Client: fakeClient, Scheme: fakeScheme}
			connectionUsers, err := reconciler.getAllRabbitMQConnectionUsernames(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(connectionUsers).To(HaveKey("admin"))
			Expect(connectionUsers).NotTo(HaveKey("app-user"))
		})

		It("should ignore deleting resources when resolving connection usernames", func() {
			host := testRabbitMQHost
			port := int32(5672)
			username := testRabbitMQUsername
			password := testRabbitMQPassword
			now := metav1.NewTime(time.Now())

			fakeClient, fakeScheme := newFakeClientWithScheme(
				&accessv1.RabbitMQAccess{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "active-rabbitmq-access",
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
				&accessv1.RabbitMQAccess{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "deleting-invalid-rabbitmq-access",
						Namespace:         "default",
						Finalizers:        []string{rabbitMQAccessFinalizer},
						DeletionTimestamp: &now,
					},
					Spec: accessv1.RabbitMQAccessSpec{
						Username:        "broken-user",
						GeneratedSecret: "broken-user-rabbitmq",
						Connection:      accessv1.ConnectionSpec{},
						Permissions: []accessv1.RabbitMQPermissionSpec{
							{VHost: "/broken", Configure: ".*", Write: ".*", Read: ".*"},
						},
					},
				},
			)

			reconciler := &AccessReconciler{Client: fakeClient, Scheme: fakeScheme}
			connectionUsers, err := reconciler.getAllRabbitMQConnectionUsernames(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(connectionUsers).To(HaveKey("admin"))
			Expect(connectionUsers).NotTo(HaveKey("broken-user"))
		})
	})

	Context("When reconciling generated credentials secrets", func() {
		It("should add finalizer for active resources", func() {
			rbq := &accessv1.RabbitMQAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "finalizer-test",
					Namespace: "default",
				},
			}

			fakeClient, fakeScheme := newFakeClientWithScheme(rbq)
			reconciler := &AccessReconciler{
				Client: fakeClient,
				Scheme: fakeScheme,
			}

			finalized, err := reconciler.finalizeRabbitMQAccess(context.Background(), rbq)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeFalse())

			updated := &accessv1.RabbitMQAccess{}
			err = fakeClient.Get(context.Background(), client.ObjectKeyFromObject(rbq), updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.ContainsFinalizer(updated, rabbitMQAccessFinalizer)).To(BeTrue())
		})

		It("should return finalized for deleting resources without finalizer", func() {
			now := metav1.NewTime(time.Now())
			rbq := &accessv1.RabbitMQAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "finalizer-test",
					Namespace:         "default",
					DeletionTimestamp: &now,
				},
			}

			reconciler := &AccessReconciler{}
			finalized, err := reconciler.finalizeRabbitMQAccess(context.Background(), rbq)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
		})

		It("should skip excluded users during finalization", func() {
			now := metav1.NewTime(time.Now())
			rbq := &accessv1.RabbitMQAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "excluded-user",
					Namespace:         "default",
					Finalizers:        []string{rabbitMQAccessFinalizer},
					DeletionTimestamp: &now,
				},
				Spec: accessv1.RabbitMQAccessSpec{
					Username:        "excluded-user",
					GeneratedSecret: "excluded-user-secret",
				},
			}

			controllerSettings := &accessv1.Controller{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cluster-settings",
					Namespace: "system",
				},
				Spec: accessv1.ControllerSpec{
					Settings: accessv1.ControllerSettings{
						RabbitMQSettings: accessv1.RabbitMQControllerSettings{
							ExcludedUsers: []string{"excluded-user"},
						},
					},
				},
			}

			fakeClient, fakeScheme := newFakeClientWithScheme(rbq, controllerSettings)
			reconciler := &AccessReconciler{
				Client: fakeClient,
				Scheme: fakeScheme,
			}

			finalized, err := reconciler.finalizeRabbitMQAccess(context.Background(), rbq)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())

			finalizedResource := &accessv1.RabbitMQAccess{}
			err = fakeClient.Get(context.Background(), client.ObjectKeyFromObject(rbq), finalizedResource)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("should set Ready=False with ConnectionError when connection details are invalid", func() {
			rbq := &accessv1.RabbitMQAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "status-error",
					Namespace: "default",
				},
				Spec: accessv1.RabbitMQAccessSpec{
					GeneratedSecret: "status-error-secret",
					Username:        "demo-user",
					Connection:      accessv1.ConnectionSpec{},
					Permissions: []accessv1.RabbitMQPermissionSpec{
						{VHost: "/app", Configure: ".*", Write: ".*", Read: ".*"},
					},
				},
			}

			fakeClient, fakeScheme := newFakeClientWithScheme(rbq)
			eventRecorder := events.NewFakeRecorder(5)
			reconciler := &AccessReconciler{
				Client:   fakeClient,
				Scheme:   fakeScheme,
				Recorder: eventRecorder,
			}

			_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: types.NamespacedName{Name: rbq.Name, Namespace: rbq.Namespace},
			})
			Expect(err).To(HaveOccurred())

			updated := &accessv1.RabbitMQAccess{}
			err = fakeClient.Get(context.Background(), types.NamespacedName{Name: rbq.Name, Namespace: rbq.Namespace}, updated)
			Expect(err).NotTo(HaveOccurred())

			readyCondition := meta.FindStatusCondition(updated.Status.Conditions, controller.ReadyConditionType)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCondition.Reason).To(Equal(rabbitMQAccessConnectionErrorReason))

			successCondition := meta.FindStatusCondition(updated.Status.Conditions, controller.SuccessConditionType)
			Expect(successCondition).NotTo(BeNil())
			Expect(successCondition.Status).To(Equal(metav1.ConditionFalse))

			inProgressCondition := meta.FindStatusCondition(updated.Status.Conditions, controller.InProgressConditionType)
			Expect(inProgressCondition).NotTo(BeNil())
			Expect(inProgressCondition.Status).To(Equal(metav1.ConditionFalse))

			Expect(updated.Status.LastReconcileState).To(Equal(accessv1.ReconcileStateError))
			Expect(updated.Status.LastLog).To(ContainSubstring("no valid connection details provided"))

			var event string
			Eventually(eventRecorder.Events).Should(Receive(&event))
			Expect(event).To(ContainSubstring(rabbitMQAccessConnectionErrorReason))
		})

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
			reconciler := &AccessReconciler{Client: fakeClient, Scheme: fakeScheme}

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
			reconciler := &AccessReconciler{Client: fakeClient, Scheme: fakeScheme}

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

		It("should ignore deleting resources when building desired RabbitMQ users", func() {
			now := metav1.NewTime(time.Now())
			active := &accessv1.RabbitMQAccess{
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
			deleting := &accessv1.RabbitMQAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "deleting-rabbitmq-access",
					Namespace:         "default",
					Finalizers:        []string{rabbitMQAccessFinalizer},
					DeletionTimestamp: &now,
				},
				Spec: accessv1.RabbitMQAccessSpec{
					Username:        "stale-user",
					GeneratedSecret: "stale-user-rabbitmq",
					Permissions: []accessv1.RabbitMQPermissionSpec{
						{VHost: "/stale", Configure: ".*", Write: ".*", Read: ".*"},
					},
				},
			}

			fakeClient, fakeScheme := newFakeClientWithScheme(active, deleting)
			reconciler := &AccessReconciler{Client: fakeClient, Scheme: fakeScheme}

			configs, err := reconciler.getAllRabbitMQUserConfigs(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(configs).To(HaveKey("app-user"))
			Expect(configs).NotTo(HaveKey("stale-user"))

			activeSecret := &corev1.Secret{}
			err = fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      "app-user-rabbitmq",
				Namespace: "default",
			}, activeSecret)
			Expect(err).NotTo(HaveOccurred())

			staleSecret := &corev1.Secret{}
			err = fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      "stale-user-rabbitmq",
				Namespace: "default",
			}, staleSecret)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})
})
