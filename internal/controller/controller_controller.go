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
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	accessv1 "github.com/delta10/access-operator/api/v1"
)

const (
	controllerReadyConditionType = "Ready"

	multipleControllersFoundReason = "MultipleControllersFound"
	invalidControllerNamespace     = "InvalidControllerNamespace"
	deploymentReconcileFailed      = "DeploymentReconcileFailed"

	managerControlPlaneLabelKey   = "control-plane"
	managerControlPlaneLabelValue = "controller-manager"
	managerAppNameLabelKey        = "app.kubernetes.io/name"
	managerAppNameLabelValue      = "access-operator"

	managerPolicyAnnotationKey = "access.k8s.delta10.nl/existing-secret-namespace"

	defaultManagerDeploymentName      = "controller-manager"
	defaultManagerDeploymentNamespace = "system"
)

// ControllerReconciler reconciles a Controller object.
type ControllerReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=controllers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=controllers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=controllers/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile enforces singleton Controller behavior across the cluster.
func (r *ControllerReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var controllers accessv1.ControllerList
	if err := r.List(ctx, &controllers); err != nil {
		return ctrl.Result{}, err
	}

	switch len(controllers.Items) {
	case 0:
		log.Info("No Controller resources found; using safe defaults")
		return ctrl.Result{}, nil
	case 1:
		controllerObj := controllers.Items[0]
		managerKey, err := r.resolveManagerDeploymentKey(ctx)
		if err != nil {
			_ = r.setControllerReadyCondition(
				ctx,
				types.NamespacedName{Name: controllerObj.Name, Namespace: controllerObj.Namespace},
				metav1.ConditionFalse,
				deploymentReconcileFailed,
				err.Error(),
			)
			return ctrl.Result{}, err
		}
		if controllerObj.Namespace != managerKey.Namespace {
			message := fmt.Sprintf(
				"Controller resource %s/%s must be created in the operator namespace %q",
				controllerObj.Namespace,
				controllerObj.Name,
				managerKey.Namespace,
			)
			_ = r.setControllerReadyCondition(
				ctx,
				types.NamespacedName{Name: controllerObj.Name, Namespace: controllerObj.Namespace},
				metav1.ConditionFalse,
				invalidControllerNamespace,
				message,
			)
			r.emitWarningEvent(&controllerObj, invalidControllerNamespace, message)
			return ctrl.Result{}, errors.New(message)
		}
		if err := r.reconcileManagerDeployment(ctx, managerKey, &controllerObj); err != nil {
			_ = r.setControllerReadyCondition(
				ctx,
				types.NamespacedName{Name: controllerObj.Name, Namespace: controllerObj.Namespace},
				metav1.ConditionFalse,
				deploymentReconcileFailed,
				err.Error(),
			)
			return ctrl.Result{}, err
		}
		if err := r.setControllerReadyCondition(
			ctx,
			types.NamespacedName{Name: controllerObj.Name, Namespace: controllerObj.Namespace},
			metav1.ConditionTrue,
			"Ready",
			"Controller singleton configuration is valid",
		); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	default:
		message := fmt.Sprintf(
			"multiple Controller resources found (%d); exactly one is allowed cluster-wide",
			len(controllers.Items),
		)
		for _, controllerObj := range controllers.Items {
			key := types.NamespacedName{Name: controllerObj.Name, Namespace: controllerObj.Namespace}
			_ = r.setControllerReadyCondition(ctx, key, metav1.ConditionFalse, multipleControllersFoundReason, message)
			r.emitWarningEvent(&controllerObj, multipleControllersFoundReason, message)
		}
		r.emitWarningOnManagerDeployments(ctx, multipleControllersFoundReason, message)

		return ctrl.Result{}, errors.New(message)
	}
}

func (r *ControllerReconciler) reconcileManagerDeployment(
	ctx context.Context,
	key types.NamespacedName,
	controllerObj *accessv1.Controller,
) error {
	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, key, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf(
				"manager deployment %s/%s not found; deploy operator manifests before configuring Controller singleton policy",
				key.Namespace,
				key.Name,
			)
		}
		return err
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		if deployment.Annotations == nil {
			deployment.Annotations = map[string]string{}
		}
		deployment.Annotations[managerPolicyAnnotationKey] = strconv.FormatBool(
			controllerObj.Spec.Settings.ExistingSecretNamespace,
		)

		return nil
	})

	return err
}

func (r *ControllerReconciler) resolveManagerDeploymentKey(ctx context.Context) (types.NamespacedName, error) {
	managerDeployments, err := listManagerDeployments(ctx, r.Client)
	if err != nil {
		return types.NamespacedName{}, err
	}

	switch len(managerDeployments) {
	case 0:
		podNamespace := os.Getenv("POD_NAMESPACE")
		if podNamespace != "" {
			return types.NamespacedName{Name: defaultManagerDeploymentName, Namespace: podNamespace}, nil
		}
		return types.NamespacedName{
			Name:      defaultManagerDeploymentName,
			Namespace: defaultManagerDeploymentNamespace,
		}, nil
	case 1:
		return types.NamespacedName{
			Name:      managerDeployments[0].Name,
			Namespace: managerDeployments[0].Namespace,
		}, nil
	default:
		podNamespace := os.Getenv("POD_NAMESPACE")
		if podNamespace != "" {
			for _, deployment := range managerDeployments {
				if deployment.Namespace == podNamespace {
					return types.NamespacedName{Name: deployment.Name, Namespace: deployment.Namespace}, nil
				}
			}
		}

		sort.Slice(managerDeployments, func(i, j int) bool {
			if managerDeployments[i].Namespace == managerDeployments[j].Namespace {
				return managerDeployments[i].Name < managerDeployments[j].Name
			}
			return managerDeployments[i].Namespace < managerDeployments[j].Namespace
		})

		return types.NamespacedName{
			Name:      managerDeployments[0].Name,
			Namespace: managerDeployments[0].Namespace,
		}, nil
	}
}

func (r *ControllerReconciler) emitWarningOnManagerDeployments(ctx context.Context, reason, message string) {
	deployments, err := listManagerDeployments(ctx, r.Client)
	if err != nil {
		return
	}

	for _, deployment := range deployments {
		deploymentCopy := deployment
		r.emitWarningEvent(
			&deploymentCopy,
			reason,
			fmt.Sprintf("%s (controller-manager deployment: %s/%s)", message, deployment.Namespace, deployment.Name),
		)
	}
}

func (r *ControllerReconciler) emitWarningEvent(object client.Object, reason, message string) {
	if r.Recorder == nil || object == nil {
		return
	}

	r.Recorder.Eventf(object, nil, corev1.EventTypeWarning, reason, "PolicyValidation", "%s", message)
}

func (r *ControllerReconciler) setControllerReadyCondition(
	ctx context.Context,
	key types.NamespacedName,
	status metav1.ConditionStatus,
	reason, message string,
) error {
	var latest accessv1.Controller
	if err := r.Get(ctx, key, &latest); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
		Type:               controllerReadyConditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: latest.GetGeneration(),
	})

	return r.Status().Update(ctx, &latest)
}

func (r *ControllerReconciler) mapManagerPodToControllers(ctx context.Context, _ client.Object) []reconcile.Request {
	var controllers accessv1.ControllerList
	if err := r.List(ctx, &controllers); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0, len(controllers.Items))
	for _, controllerObj := range controllers.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      controllerObj.Name,
				Namespace: controllerObj.Namespace,
			},
		})
	}

	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *ControllerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&accessv1.Controller{}).
		Owns(&appsv1.Deployment{}).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.mapManagerPodToControllers),
			builder.WithPredicates(predicate.NewPredicateFuncs(func(object client.Object) bool {
				return object.GetLabels()[managerControlPlaneLabelKey] == managerControlPlaneLabelValue
			})),
		).
		Named("controller").
		Complete(r)
}
