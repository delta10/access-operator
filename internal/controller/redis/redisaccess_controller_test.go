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

package redis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

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

const testRedisHost = "redis.default.svc.cluster.local"

const (
	testRedisAdminUsername = "default"
	testRedisAdminPassword = "secret"
	testRedisSecretName    = "redis-connection-secret"
)

var _ = Describe("RedisAccess Controller", func() {
	Context("When reconciling RedisAccess resources with envtest", func() {
		const resourceName = "redis-access-test"

		ctx := context.Background()
		typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

		AfterEach(func() {
			resource := &accessv1.RedisAccess{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				By("cleaning up the RedisAccess resource")
				if controllerutil.ContainsFinalizer(resource, redisAccessFinalizer) {
					controllerutil.RemoveFinalizer(resource, redisAccessFinalizer)
					Expect(k8sClient.Update(ctx, resource)).To(Succeed())
				}
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
				Eventually(func() bool {
					getErr := k8sClient.Get(ctx, typeNamespacedName, &accessv1.RedisAccess{})
					return apierrors.IsNotFound(getErr)
				}).Should(BeTrue())
			}

			By("cleaning up test secrets")
			for _, secretName := range []string{"redis-generated-secret", "redis-connection-secret"} {
				_ = k8sClient.Delete(ctx, &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: "default"},
				})
			}
		})

		It("should create a generated secret, apply Redis ACLs, and reach Ready=True once state is synced", func() {
			host := testRedisHost
			port := int32(6379)
			adminUsername := testRedisAdminUsername
			adminPassword := testRedisAdminPassword

			resource := &accessv1.RedisAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: accessv1.RedisAccessSpec{
					GeneratedSecret: "redis-generated-secret",
					Username:        "app-user",
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Username: &accessv1.SecretKeySelector{Value: &adminUsername},
						Password: &accessv1.SecretKeySelector{Value: &adminPassword},
					},
					ACLRules: []string{"~cache:*", "+@read", "+@write"},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			mockState := newMockACLState()
			reconciler := &RedisAccessReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				NewACLClient: newMockACLFactory(mockState),
			}

			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(controller.PrivilegeDriftRequeueInterval))
			Expect(mockState.setCalls()).To(HaveLen(2))

			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "redis-generated-secret", Namespace: "default"}, secret)).To(Succeed())
			Expect(string(secret.Data["username"])).To(Equal("app-user"))
			Expect(string(secret.Data["password"])).NotTo(BeEmpty())

			updated := &accessv1.RedisAccess{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			Expect(updated.Status.LastReconcileState).To(Equal(accessv1.ReconcileStateInProgress))
			Expect(meta.FindStatusCondition(updated.Status.Conditions, controller.ReadyConditionType).Status).To(Equal(metav1.ConditionFalse))

			result, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(controller.SyncedRequeueInterval))
			Expect(mockState.setCalls()).To(HaveLen(3))

			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			Expect(updated.Status.LastReconcileState).To(Equal(accessv1.ReconcileStateSuccess))
			Expect(meta.FindStatusCondition(updated.Status.Conditions, controller.ReadyConditionType).Status).To(Equal(metav1.ConditionTrue))
			Expect(mockState.userRules("app-user")).To(ContainSubstring(">" + string(secret.Data["password"])))
		})

		It("should resolve connection details from an existing secret and create the Redis ACL user", func() {
			secretName := testRedisSecretName
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: "default"},
				Data: map[string][]byte{
					"host":     []byte("redis"),
					"port":     []byte(strconv.Itoa(6379)),
					"username": []byte("default"),
					"password": []byte("secret"),
				},
			})).To(Succeed())

			resource := &accessv1.RedisAccess{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
				Spec: accessv1.RedisAccessSpec{
					GeneratedSecret: "redis-generated-secret",
					Username:        "app-user",
					Connection: accessv1.ConnectionSpec{
						ExistingSecret: &secretName,
					},
					ACLRules: []string{"~orders:*", "+get"},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			mockState := newMockACLState()
			reconciler := &RedisAccessReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				NewACLClient: newMockACLFactory(mockState),
			}

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(mockState.users()).To(ContainElement("app-user"))
		})
	})

	Context("When testing helper logic with fake clients", func() {
		It("should build Redis connection details from direct connection fields", func() {
			host := testRedisHost
			port := int32(6379)
			username := "default"
			password := "secret"

			reconciler := &RedisAccessReconciler{}
			redisAccess := &accessv1.RedisAccess{
				Spec: accessv1.RedisAccessSpec{
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Username: &accessv1.SecretKeySelector{Value: &username},
						Password: &accessv1.SecretKeySelector{Value: &password},
					},
				},
			}

			connection, err := reconciler.getConnectionDetails(context.Background(), redisAccess)
			Expect(err).NotTo(HaveOccurred())
			Expect(connection.Host).To(Equal(testRedisHost))
			Expect(connection.Port).To(Equal("6379"))
			Expect(connection.Username).To(Equal("default"))
			Expect(connection.Password).To(Equal("secret"))
		})

		It("should build Redis connection details from an existing secret", func() {
			secretName := testRedisSecretName
			fakeClient, _ := controller.NewFakeClientWithScheme(
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: "default"},
					Data: map[string][]byte{
						"host":     []byte("redis"),
						"port":     []byte("6379"),
						"username": []byte(testRedisAdminUsername),
						"password": []byte(testRedisAdminPassword),
					},
				},
			)

			reconciler := &RedisAccessReconciler{Client: fakeClient}
			redisAccess := &accessv1.RedisAccess{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: accessv1.RedisAccessSpec{
					Connection: accessv1.ConnectionSpec{
						ExistingSecret: &secretName,
					},
				},
			}

			connection, err := reconciler.getConnectionDetails(context.Background(), redisAccess)
			Expect(err).NotTo(HaveOccurred())
			Expect(connection.Host).To(Equal("redis.default.svc"))
			Expect(connection.Port).To(Equal("6379"))
			Expect(connection.Username).To(Equal(testRedisAdminUsername))
			Expect(connection.Password).To(Equal(testRedisAdminPassword))
		})

		It("should allow cross-namespace existingSecret when singleton Controller policy is enabled", func() {
			secretName := testRedisSecretName
			secretNamespace := "shared-redis"

			fakeClient, _ := controller.NewFakeClientWithScheme(
				&accessv1.Controller{
					ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "system"},
					Spec: accessv1.ControllerSpec{
						Settings: accessv1.ControllerSettings{ExistingSecretNamespace: true},
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: secretNamespace},
					Data: map[string][]byte{
						"host":     []byte("redis"),
						"port":     []byte("6379"),
						"username": []byte(testRedisAdminUsername),
						"password": []byte(testRedisAdminPassword),
					},
				},
			)

			reconciler := &RedisAccessReconciler{Client: fakeClient}
			redisAccess := &accessv1.RedisAccess{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: accessv1.RedisAccessSpec{
					Connection: accessv1.ConnectionSpec{
						ExistingSecret:          &secretName,
						ExistingSecretNamespace: &secretNamespace,
					},
				},
			}

			connection, err := reconciler.getConnectionDetails(context.Background(), redisAccess)
			Expect(err).NotTo(HaveOccurred())
			Expect(connection.Host).To(Equal("redis.shared-redis.svc"))
		})

		It("should hard fail cross-namespace existingSecret when multiple Controller resources exist", func() {
			secretName := "redis-connection-secret"
			secretNamespace := "shared-redis"

			fakeClient, _ := controller.NewFakeClientWithScheme(
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
					ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: secretNamespace},
					Data: map[string][]byte{
						"host":     []byte("redis"),
						"port":     []byte("6379"),
						"username": []byte("default"),
						"password": []byte("secret"),
					},
				},
			)

			reconciler := &RedisAccessReconciler{Client: fakeClient}
			redisAccess := &accessv1.RedisAccess{
				ObjectMeta: metav1.ObjectMeta{Name: "cross-namespace", Namespace: "default"},
				Spec: accessv1.RedisAccessSpec{
					Connection: accessv1.ConnectionSpec{
						ExistingSecret:          &secretName,
						ExistingSecretNamespace: &secretNamespace,
					},
				},
			}

			_, err := reconciler.getConnectionDetails(context.Background(), redisAccess)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("multiple Controller resources found"))
		})

		It("should normalize excluded usernames from singleton Controller settings", func() {
			fakeClient, _ := controller.NewFakeClientWithScheme(
				&accessv1.Controller{
					ObjectMeta: metav1.ObjectMeta{Name: "cluster-settings", Namespace: "system"},
					Spec: accessv1.ControllerSpec{
						Settings: accessv1.ControllerSettings{
							RedisSettings: accessv1.RedisControllerSettings{
								ExcludedUsers: []string{" default ", "", "ops-user", "default"},
							},
						},
					},
				},
			)

			reconciler := &RedisAccessReconciler{Client: fakeClient}
			excludedUsers, err := reconciler.resolveExcludedUsers(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(excludedUsers).To(HaveLen(2))
			Expect(excludedUsers).To(HaveKey("default"))
			Expect(excludedUsers).To(HaveKey("ops-user"))
		})

		It("should reject reserved ACL directives in user rules", func() {
			Expect(validateACLRules([]string{"~cache:*", "+get"})).To(Succeed())
			Expect(validateACLRules([]string{"on"})).To(MatchError(ContainSubstring("reserved")))
			Expect(validateACLRules([]string{">password"})).To(MatchError(ContainSubstring("reserved")))
			Expect(validateACLRules([]string{"nopass"})).To(MatchError(ContainSubstring("reserved")))
		})

		It("should set Ready=False and emit an event when connection details are invalid", func() {
			redisAccess := &accessv1.RedisAccess{
				ObjectMeta: metav1.ObjectMeta{Name: "status-error", Namespace: "default"},
				Spec: accessv1.RedisAccessSpec{
					GeneratedSecret: "status-error-secret",
					Username:        "demo-user",
					Connection:      accessv1.ConnectionSpec{},
					ACLRules:        []string{"~cache:*", "+get"},
				},
			}

			fakeClient, fakeScheme := controller.NewFakeClientWithScheme(redisAccess)
			eventRecorder := events.NewFakeRecorder(5)
			reconciler := &RedisAccessReconciler{
				Client:       fakeClient,
				Scheme:       fakeScheme,
				Recorder:     eventRecorder,
				NewACLClient: newMockACLFactory(newMockACLState()),
			}

			_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(redisAccess),
			})
			Expect(err).To(HaveOccurred())

			updated := &accessv1.RedisAccess{}
			Expect(fakeClient.Get(context.Background(), client.ObjectKeyFromObject(redisAccess), updated)).To(Succeed())

			readyCondition := meta.FindStatusCondition(updated.Status.Conditions, controller.ReadyConditionType)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCondition.Reason).To(Equal(redisAccessConnectionErrorReason))
			Expect(updated.Status.LastReconcileState).To(Equal(accessv1.ReconcileStateError))
			Expect(updated.Status.LastLog).To(ContainSubstring("no valid connection details provided"))

			event := controller.ReceiveEvents(eventRecorder.Events, 1)
			Expect(event).To(ContainSubstring(redisAccessConnectionErrorReason))
		})

		It("should only update Redis when ACL state is drifted", func() {
			password := "managed-password"
			mockState := newMockACLState()
			Expect(mockState.SetUser(context.Background(), "app-user", buildManagedACLRules(password, []string{"~cache:*", "+get"})...)).To(Succeed())

			expectedACL, err := (&RedisAccessReconciler{}).canonicalizeDesiredACL(
				context.Background(),
				&mockACLClient{state: mockState},
				password,
				[]string{"~cache:*", "+get"},
			)
			Expect(err).NotTo(HaveOccurred())

			currentACL := aclLinesByUser(mockState.ListACLLines())["app-user"]
			Expect(currentACL).To(Equal(expectedACL))
		})

		It("should skip finalizer user deletion for excluded usernames", func() {
			now := metav1.Now()
			mockState := newMockACLState()
			Expect(mockState.SetUser(context.Background(), "default", "reset", "on", ">secret", "~*", "+@all")).To(Succeed())

			redisAccess := &accessv1.RedisAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "excluded-user",
					Namespace:         "default",
					DeletionTimestamp: &now,
					Finalizers:        []string{redisAccessFinalizer},
				},
				Spec: accessv1.RedisAccessSpec{
					GeneratedSecret: "excluded-secret",
					Username:        "default",
					ACLRules:        []string{"~*", "+@all"},
				},
			}

			fakeClient, fakeScheme := controller.NewFakeClientWithScheme(
				redisAccess,
				&accessv1.Controller{
					ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "system"},
					Spec: accessv1.ControllerSpec{
						Settings: accessv1.ControllerSettings{
							RedisSettings: accessv1.RedisControllerSettings{
								ExcludedUsers: []string{"default"},
							},
						},
					},
				},
			)

			reconciler := &RedisAccessReconciler{
				Client:       fakeClient,
				Scheme:       fakeScheme,
				NewACLClient: newMockACLFactory(mockState),
			}

			finalized, err := reconciler.finalizeRedisAccess(context.Background(), redisAccess)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
			Expect(mockState.deleteCalls()).To(BeEmpty())
		})

		It("should skip finalizer user deletion when the username is still used for Redis connections", func() {
			now := metav1.Now()
			host := testRedisHost
			port := int32(6379)
			connectionUser := testRedisAdminUsername
			connectionPassword := testRedisAdminPassword
			mockState := newMockACLState()
			Expect(mockState.SetUser(context.Background(), connectionUser, "reset", "on", ">secret", "~*", "+@all")).To(Succeed())

			deleting := &accessv1.RedisAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "deleting",
					Namespace:         "default",
					DeletionTimestamp: &now,
					Finalizers:        []string{redisAccessFinalizer},
				},
				Spec: accessv1.RedisAccessSpec{
					GeneratedSecret: "deleting-secret",
					Username:        connectionUser,
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Username: &accessv1.SecretKeySelector{Value: &connectionUser},
						Password: &accessv1.SecretKeySelector{Value: &connectionPassword},
					},
					ACLRules: []string{"~cache:*", "+get"},
				},
			}
			other := &accessv1.RedisAccess{
				ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default"},
				Spec: accessv1.RedisAccessSpec{
					GeneratedSecret: "other-secret",
					Username:        "app-user",
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Username: &accessv1.SecretKeySelector{Value: &connectionUser},
						Password: &accessv1.SecretKeySelector{Value: &connectionPassword},
					},
					ACLRules: []string{"~cache:*", "+get"},
				},
			}

			fakeClient, fakeScheme := controller.NewFakeClientWithScheme(deleting, other)
			reconciler := &RedisAccessReconciler{
				Client:       fakeClient,
				Scheme:       fakeScheme,
				NewACLClient: newMockACLFactory(mockState),
			}

			finalized, err := reconciler.finalizeRedisAccess(context.Background(), deleting)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
			Expect(mockState.deleteCalls()).To(BeEmpty())
		})

		It("should skip finalizer user deletion when another RedisAccess still manages the username", func() {
			now := metav1.Now()
			host := testRedisHost
			port := int32(6379)
			adminUsername := testRedisAdminUsername
			adminPassword := testRedisAdminPassword
			mockState := newMockACLState()
			Expect(mockState.SetUser(context.Background(), "shared-user", "reset", "on", ">secret", "~cache:*", "+get")).To(Succeed())

			deleting := &accessv1.RedisAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "deleting",
					Namespace:         "default",
					DeletionTimestamp: &now,
					Finalizers:        []string{redisAccessFinalizer},
				},
				Spec: accessv1.RedisAccessSpec{
					GeneratedSecret: "deleting-secret",
					Username:        "shared-user",
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Username: &accessv1.SecretKeySelector{Value: &adminUsername},
						Password: &accessv1.SecretKeySelector{Value: &adminPassword},
					},
					ACLRules: []string{"~cache:*", "+get"},
				},
			}
			other := &accessv1.RedisAccess{
				ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default"},
				Spec: accessv1.RedisAccessSpec{
					GeneratedSecret: "other-secret",
					Username:        "shared-user",
					Connection: accessv1.ConnectionSpec{
						Host:     &host,
						Port:     &port,
						Username: &accessv1.SecretKeySelector{Value: &adminUsername},
						Password: &accessv1.SecretKeySelector{Value: &adminPassword},
					},
					ACLRules: []string{"~orders:*", "+set"},
				},
			}

			fakeClient, fakeScheme := controller.NewFakeClientWithScheme(deleting, other)
			reconciler := &RedisAccessReconciler{
				Client:       fakeClient,
				Scheme:       fakeScheme,
				NewACLClient: newMockACLFactory(mockState),
			}

			finalized, err := reconciler.finalizeRedisAccess(context.Background(), deleting)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
			Expect(mockState.deleteCalls()).To(BeEmpty())
		})
	})
})

type mockACLState struct {
	mu          sync.Mutex
	usersByName map[string]string
	setOps      []string
	delOps      []string
	pingErr     error
}

func newMockACLState() *mockACLState {
	return &mockACLState{
		usersByName: map[string]string{},
	}
}

func (s *mockACLState) setCalls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.setOps...)
}

func (s *mockACLState) deleteCalls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.delOps...)
}

func (s *mockACLState) users() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return sortedKeys(s.usersByName)
}

func (s *mockACLState) userRules(username string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usersByName[username]
}

func (s *mockACLState) ListACLLines() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	lines := make([]string, 0, len(s.usersByName))
	for _, username := range sortedKeys(s.usersByName) {
		lines = append(lines, fmt.Sprintf("user %s %s", username, s.usersByName[username]))
	}
	return lines
}

func (s *mockACLState) Ping(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pingErr
}

func (s *mockACLState) ListUsers(context.Context) ([]string, error) {
	return s.users(), nil
}

func (s *mockACLState) ListACL(context.Context) ([]string, error) {
	return s.ListACLLines(), nil
}

func (s *mockACLState) SetUser(_ context.Context, username string, rules ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usersByName[username] = normalizeMockACLRules(rules)
	s.setOps = append(s.setOps, username)
	return nil
}

func (s *mockACLState) DeleteUser(_ context.Context, username string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.usersByName[username]; !ok {
		return 0, nil
	}
	delete(s.usersByName, username)
	s.delOps = append(s.delOps, username)
	return 1, nil
}

type mockACLClient struct {
	state *mockACLState
}

func (m *mockACLClient) Ping(ctx context.Context) error {
	return m.state.Ping(ctx)
}

func (m *mockACLClient) ListUsers(ctx context.Context) ([]string, error) {
	return m.state.ListUsers(ctx)
}

func (m *mockACLClient) ListACL(ctx context.Context) ([]string, error) {
	return m.state.ListACL(ctx)
}

func (m *mockACLClient) SetUser(ctx context.Context, username string, rules ...string) error {
	return m.state.SetUser(ctx, username, rules...)
}

func (m *mockACLClient) DeleteUser(ctx context.Context, username string) (int64, error) {
	return m.state.DeleteUser(ctx, username)
}

func (m *mockACLClient) Close() error {
	return nil
}

func newMockACLFactory(state *mockACLState) ClientFactory {
	return func(_ controller.SharedConnectionDetails) ACLClient {
		return &mockACLClient{state: state}
	}
}

func normalizeMockACLRules(rules []string) string {
	finalRules := make([]string, 0, len(rules))
	for _, rule := range rules {
		trimmed := strings.TrimSpace(rule)
		if trimmed == "" {
			continue
		}
		if trimmed == "reset" {
			finalRules = finalRules[:0]
			continue
		}
		finalRules = append(finalRules, trimmed)
	}
	return strings.Join(finalRules, " ")
}

func sortedKeys[V any](items map[string]V) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
