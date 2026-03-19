package controller

import (
	accessv1 "github.com/delta10/access-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	. "github.com/onsi/gomega"
)

func NewFakeClientWithScheme(objs ...client.Object) (client.Client, *runtime.Scheme) {
	scheme := runtime.NewScheme()
	Expect(accessv1.AddToScheme(scheme)).To(Succeed())
	Expect(corev1.AddToScheme(scheme)).To(Succeed())
	Expect(appsv1.AddToScheme(scheme)).To(Succeed())

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&accessv1.PostgresAccess{}, &accessv1.RabbitMQAccess{}, &accessv1.Controller{}).
		WithObjects(objs...).
		Build()

	return fakeClient, scheme
}
