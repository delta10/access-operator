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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

var _ = Describe("Controller Controller", func() {
	ctx := context.Background()

	It("should fail reconciliation, mark all Controller resources not ready, and emit warning events when multiples exist", func() {
		controllerAKey := types.NamespacedName{Name: "controller-a", Namespace: "system"}
		controllerBKey := types.NamespacedName{Name: "controller-b", Namespace: "default"}

		fakeClient, fakeScheme := NewFakeClientWithScheme(
			&accessv1.Controller{
				ObjectMeta: metav1.ObjectMeta{Name: controllerAKey.Name, Namespace: controllerAKey.Namespace},
			},
			&accessv1.Controller{
				ObjectMeta: metav1.ObjectMeta{Name: controllerBKey.Name, Namespace: controllerBKey.Namespace},
			},
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "access-operator-controller-manager",
					Namespace: "access-operator-system",
					Labels: map[string]string{
						managerControlPlaneLabelKey: managerControlPlaneLabelValue,
						managerAppNameLabelKey:      managerAppNameLabelValue,
					},
				},
			},
		)

		eventRecorder := events.NewFakeRecorder(20)
		reconciler := &ControllerReconciler{
			Client:   fakeClient,
			Scheme:   fakeScheme,
			Recorder: eventRecorder,
		}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: controllerAKey})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("multiple Controller resources found"))

		for _, key := range []types.NamespacedName{controllerAKey, controllerBKey} {
			controllerObj := &accessv1.Controller{}
			Expect(fakeClient.Get(ctx, key, controllerObj)).To(Succeed())

			readyCondition := meta.FindStatusCondition(controllerObj.Status.Conditions, controllerReadyConditionType)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCondition.Reason).To(Equal(MultipleControllersFoundReason))
		}

		var eventOne, eventTwo, eventThree string
		Eventually(eventRecorder.Events).Should(Receive(&eventOne))
		Eventually(eventRecorder.Events).Should(Receive(&eventTwo))
		Eventually(eventRecorder.Events).Should(Receive(&eventThree))

		allEvents := strings.Join([]string{eventOne, eventTwo, eventThree}, " ")
		Expect(allEvents).To(ContainSubstring(MultipleControllersFoundReason))
		Expect(allEvents).To(ContainSubstring("controller-manager deployment: access-operator-system/access-operator-controller-manager"))
	})

	It("should reconcile the manager deployment idempotently for the singleton Controller", func() {
		controllerKey := types.NamespacedName{Name: "cluster-settings", Namespace: "system"}
		deploymentKey := client.ObjectKey{
			Name:      defaultManagerDeploymentName,
			Namespace: defaultManagerDeploymentNamespace,
		}
		fakeClient, fakeScheme := NewFakeClientWithScheme(
			&accessv1.Controller{
				ObjectMeta: metav1.ObjectMeta{Name: controllerKey.Name, Namespace: controllerKey.Namespace},
				Spec: accessv1.ControllerSpec{
					Settings: accessv1.ControllerSettings{
						ExistingSecretNamespace: true,
					},
				},
			},
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deploymentKey.Name,
					Namespace: deploymentKey.Namespace,
					Labels: map[string]string{
						managerControlPlaneLabelKey: managerControlPlaneLabelValue,
						managerAppNameLabelKey:      managerAppNameLabelValue,
					},
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app": "manager",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"app": "manager"},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:  "manager",
								Image: "example.com/access-operator:v0.0.1",
							}},
						},
					},
				},
			},
		)

		reconciler := &ControllerReconciler{
			Client: fakeClient,
			Scheme: fakeScheme,
		}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: controllerKey})
		Expect(err).NotTo(HaveOccurred())

		firstDeployment := &appsv1.Deployment{}
		Expect(fakeClient.Get(ctx, deploymentKey, firstDeployment)).To(Succeed())
		Expect(firstDeployment.Annotations).To(HaveKeyWithValue(managerPolicyAnnotationKey, "true"))
		firstCopy := firstDeployment.DeepCopy()

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: controllerKey})
		Expect(err).NotTo(HaveOccurred())

		secondDeployment := &appsv1.Deployment{}
		Expect(fakeClient.Get(ctx, deploymentKey, secondDeployment)).To(Succeed())
		Expect(secondDeployment.Annotations).To(HaveKeyWithValue(managerPolicyAnnotationKey, "true"))
		Expect(secondDeployment.Spec).To(Equal(firstCopy.Spec))
		Expect(secondDeployment.Annotations).To(Equal(firstCopy.Annotations))

		var deploymentList appsv1.DeploymentList
		Expect(fakeClient.List(ctx, &deploymentList)).To(Succeed())
		Expect(deploymentList.Items).To(HaveLen(1))
	})

	It("should reject a singleton Controller outside the operator namespace", func() {
		controllerKey := types.NamespacedName{Name: "cluster-settings", Namespace: "default"}
		deploymentKey := client.ObjectKey{
			Name:      "access-operator-controller-manager",
			Namespace: "access-operator-system",
		}
		fakeClient, fakeScheme := NewFakeClientWithScheme(
			&accessv1.Controller{
				ObjectMeta: metav1.ObjectMeta{Name: controllerKey.Name, Namespace: controllerKey.Namespace},
				Spec: accessv1.ControllerSpec{
					Settings: accessv1.ControllerSettings{
						ExistingSecretNamespace: true,
					},
				},
			},
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deploymentKey.Name,
					Namespace: deploymentKey.Namespace,
					Labels: map[string]string{
						managerControlPlaneLabelKey: managerControlPlaneLabelValue,
						managerAppNameLabelKey:      managerAppNameLabelValue,
					},
				},
			},
		)

		eventRecorder := events.NewFakeRecorder(10)
		reconciler := &ControllerReconciler{
			Client:   fakeClient,
			Scheme:   fakeScheme,
			Recorder: eventRecorder,
		}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: controllerKey})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`must be created in the operator namespace "access-operator-system"`))

		controllerObj := &accessv1.Controller{}
		Expect(fakeClient.Get(ctx, controllerKey, controllerObj)).To(Succeed())

		readyCondition := meta.FindStatusCondition(controllerObj.Status.Conditions, controllerReadyConditionType)
		Expect(readyCondition).NotTo(BeNil())
		Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
		Expect(readyCondition.Reason).To(Equal(invalidControllerNamespace))

		var event string
		Eventually(eventRecorder.Events).Should(Receive(&event))
		Expect(event).To(ContainSubstring(invalidControllerNamespace))
	})
})
