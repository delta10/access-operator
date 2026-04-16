package controller

import (
	"context"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

var _ = Describe("Shared config logic", func() {
	Context("ResolveControllerSettings", func() {
		It("should return zero settings when ConfigMap does not exist", func() {
			fakeClient := newFakeClientWithScheme()

			settings, err := ResolveControllerSettings(context.Background(), fakeClient)
			Expect(err).NotTo(HaveOccurred())
			Expect(settings).To(Equal(accessv1.ControllerSettings{}))
		})

		It("should parse settings from ConfigMap payload", func() {
			fakeClient := newFakeClientWithScheme(
				newControllerSettingsConfigMap("system", accessv1.ControllerSettings{
					ExistingSecretNamespace: true,
					PostgresSettings: accessv1.PostgresControllerSettings{
						ExcludedUsers: []string{"postgres"},
					},
				}),
			)

			settings, err := ResolveControllerSettings(context.Background(), fakeClient)
			Expect(err).NotTo(HaveOccurred())
			Expect(settings.ExistingSecretNamespace).To(BeTrue())
			Expect(settings.PostgresSettings.ExcludedUsers).To(Equal([]string{"postgres"}))
		})

		It("should ignore ConfigMap outside operator namespace", func() {
			fakeClient := newFakeClientWithScheme(
				newControllerSettingsConfigMap("tenant-a", accessv1.ControllerSettings{
					ExistingSecretNamespace: true,
				}),
			)

			settings, err := ResolveControllerSettings(context.Background(), fakeClient)
			Expect(err).NotTo(HaveOccurred())
			Expect(settings).To(Equal(accessv1.ControllerSettings{}))
		})

		It("should return parse error for malformed ConfigMap data", func() {
			fakeClient := newFakeClientWithScheme(
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ControllerSettingsConfigMapName,
						Namespace: "system",
					},
					Data: map[string]string{
						ControllerSettingsConfigMapKey: "existingSecretNamespace: [",
					},
				},
			)

			_, err := ResolveControllerSettings(context.Background(), fakeClient)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to parse ConfigMap system/access-operator-settings"))
		})
	})

	Context("IsControllerSettingsConfigMap", func() {
		It("should match only fixed ConfigMap name", func() {
			Expect(IsControllerSettingsConfigMap(&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: ControllerSettingsConfigMapName},
			})).To(BeTrue())

			Expect(IsControllerSettingsConfigMap(&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "other"},
			})).To(BeFalse())
		})
	})
})

func newFakeClientWithScheme(objs ...client.Object) client.Client {
	testScheme := runtime.NewScheme()
	Expect(accessv1.AddToScheme(testScheme)).To(Succeed())
	Expect(corev1.AddToScheme(testScheme)).To(Succeed())
	Expect(appsv1.AddToScheme(testScheme)).To(Succeed())

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithStatusSubresource(&accessv1.PostgresAccess{}, &accessv1.RabbitMQAccess{}, &accessv1.RedisAccess{}).
		WithObjects(objs...).
		Build()

	return fakeClient
}

func newControllerSettingsConfigMap(namespace string, settings accessv1.ControllerSettings) *corev1.ConfigMap {
	rawConfig, err := yaml.Marshal(settings)
	Expect(err).NotTo(HaveOccurred())

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ControllerSettingsConfigMapName,
			Namespace: namespace,
		},
		Data: map[string]string{
			ControllerSettingsConfigMapKey: string(rawConfig),
		},
	}
}

func newControllerSettingsConfigMapWithRawData(namespace, rawData string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ControllerSettingsConfigMapName,
			Namespace: namespace,
		},
		Data: map[string]string{
			ControllerSettingsConfigMapKey: strings.TrimSpace(rawData),
		},
	}
}
