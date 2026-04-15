package test

import (
	"fmt"
	"strings"

	accessv1 "github.com/delta10/access-operator/api/v1"
	operatorcontroller "github.com/delta10/access-operator/internal/controller"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"
)

func NewFakeClientWithScheme(objs ...client.Object) (client.Client, *runtime.Scheme) {
	testScheme := runtime.NewScheme()
	gomega.Expect(accessv1.AddToScheme(testScheme)).To(gomega.Succeed())
	gomega.Expect(corev1.AddToScheme(testScheme)).To(gomega.Succeed())
	gomega.Expect(appsv1.AddToScheme(testScheme)).To(gomega.Succeed())

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithStatusSubresource(&accessv1.PostgresAccess{}, &accessv1.RabbitMQAccess{}, &accessv1.RedisAccess{}).
		WithObjects(objs...).
		Build()

	return fakeClient, testScheme
}

func ReceiveEvents(events <-chan string, count int) string {
	received := make([]string, 0, count)
	for range count {
		var event string
		gomega.Eventually(events).Should(gomega.Receive(&event))
		received = append(received, event)
	}

	return strings.Join(received, " ")
}

func NewControllerSettingsConfigMap(namespace string, settings accessv1.ControllerSettings) *corev1.ConfigMap {
	rawConfig, err := yaml.Marshal(map[string]accessv1.ControllerSpec{
		"spec": {
			Settings: settings,
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      operatorcontroller.ControllerSettingsConfigMapName,
			Namespace: namespace,
		},
		Data: map[string]string{
			operatorcontroller.ControllerSettingsConfigMapKey: string(rawConfig),
		},
	}
}

func NewControllerSettingsConfigMapWithRawData(namespace, rawData string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      operatorcontroller.ControllerSettingsConfigMapName,
			Namespace: namespace,
		},
		Data: map[string]string{
			operatorcontroller.ControllerSettingsConfigMapKey: strings.TrimSpace(rawData),
		},
	}
}

func DescribeControllerSettingsConfigMap(namespace string) string {
	return fmt.Sprintf("%s/%s", namespace, operatorcontroller.ControllerSettingsConfigMapName)
}
