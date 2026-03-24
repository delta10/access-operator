package controller

import (
	"strings"

	accessv1 "github.com/delta10/access-operator/api/v1"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func NewFakeClientWithScheme(objs ...client.Object) (client.Client, *runtime.Scheme) {
	testScheme := runtime.NewScheme()
	gomega.Expect(accessv1.AddToScheme(testScheme)).To(gomega.Succeed())
	gomega.Expect(corev1.AddToScheme(testScheme)).To(gomega.Succeed())
	gomega.Expect(appsv1.AddToScheme(testScheme)).To(gomega.Succeed())

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithStatusSubresource(&accessv1.PostgresAccess{}, &accessv1.RabbitMQAccess{}, &accessv1.RedisAccess{}, &accessv1.Controller{}).
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
