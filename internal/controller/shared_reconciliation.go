package controller

import (
	"context"
	"fmt"
	"time"

	accessv1 "github.com/delta10/access-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// PrivilegeDriftRequeueInterval is how long it takes when we detect a privilege drift before we check again for changes in the service that we need to reconcile.
// Periodic checks are necessary because the actual services don't give a reconcile signal when they drift.
const PrivilegeDriftRequeueInterval = 30 * time.Second

// SyncedRequeueInterval is how long it takes when everything is synced before we check again for changes in the service that we need to reconcile.
// Periodic checks are necessary because the actual services don't give a reconcile signal when they drift.
const SyncedRequeueInterval = 5 * time.Minute

const AccessResourceFinalizer = "access.k8s.delta10.nl/finalizer"

type ReconcileConditionTypes struct {
	Ready      string
	Success    string
	InProgress string
}

const (
	ReadyConditionType      = "Ready"
	SuccessConditionType    = "ReconcileSuccess"
	InProgressConditionType = "ReconcileInProgress"

	SecretSyncErrorEventReason = "SecretSyncFailed"
)

type ReconcileStatusConfig[T client.Object] struct {
	NewObject             func() T
	Conditions            func(T) *[]metav1.Condition
	SetLastLog            func(T, string)
	SetLastReconcileState func(T, accessv1.ReconcileState)
	ConditionTypes        ReconcileConditionTypes
}

type ManagedAccessReconcileConfig[T client.Object] struct {
	Client client.Client
	Scheme *runtime.Scheme

	StatusConfig ReconcileStatusConfig[T]
	Finalize     func(context.Context, T) (bool, error)
	// Sync is the function used to reconcile the backend service.
	Sync       func(context.Context, T) (bool, string, error)
	SecretName func(T) string
	Username   func(T) string
	EmitEvent  func(T, string, string, string)

	FinalizeErrorReason string
	SyncErrorReason     string
	SyncErrorEventText  func(T, string, error) string
	InProgressMessage   string
	SuccessMessage      string
	SuccessEventReason  string
	SuccessEventMessage string
}

func NewStandardReconcileStatusConfig[T client.Object](
	newObject func() T,
	conditions func(T) *[]metav1.Condition,
	setLastLog func(T, string),
	setLastReconcileState func(T, accessv1.ReconcileState),
) ReconcileStatusConfig[T] {
	return ReconcileStatusConfig[T]{
		NewObject:             newObject,
		Conditions:            conditions,
		SetLastLog:            setLastLog,
		SetLastReconcileState: setLastReconcileState,
		ConditionTypes: ReconcileConditionTypes{
			Ready:      ReadyConditionType,
			Success:    SuccessConditionType,
			InProgress: InProgressConditionType,
		},
	}
}

func ReconcileManagedAccess[T client.Object](
	ctx context.Context,
	req ctrl.Request,
	config ManagedAccessReconcileConfig[T],
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	obj := config.StatusConfig.NewObject()
	if err := config.Client.Get(ctx, req.NamespacedName, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	emitEvent := func(eventType, reason, message string) {
		if config.EmitEvent == nil {
			return
		}
		config.EmitEvent(obj, eventType, reason, message)
	}

	inSync := true

	finalized, err := config.Finalize(ctx, obj)
	if err != nil {
		message := fmt.Sprintf("Finalization failed: %v", err)
		emitEvent(corev1.EventTypeWarning, config.FinalizeErrorReason, message)
		statusErr := SetReconcileStatus(
			ctx,
			config.Client,
			req.NamespacedName,
			config.StatusConfig,
			accessv1.ReconcileStateError,
			config.FinalizeErrorReason,
			message,
		)
		if statusErr != nil {
			log.Error(statusErr, "failed to update status after finalization failure")
		}
		return ctrl.Result{}, err
	}
	if finalized {
		return ctrl.Result{}, nil
	}

	_, passwordReused, err := ReconcileGeneratedCredentialsSecret(
		ctx,
		config.Client,
		config.Scheme,
		obj,
		config.SecretName(obj),
		req.Namespace,
		config.Username(obj),
	)
	if err != nil {
		log.Error(err, "failed to create/update secret", "secret", config.SecretName(obj))
		emitEvent(corev1.EventTypeWarning, SecretSyncErrorEventReason, err.Error())
		statusErr := SetReconcileStatus(
			ctx,
			config.Client,
			req.NamespacedName,
			config.StatusConfig,
			accessv1.ReconcileStateError,
			SecretSyncErrorEventReason,
			err.Error(),
		)
		if statusErr != nil {
			log.Error(statusErr, "failed to update status after secret sync failure")
		}
		return ctrl.Result{}, err
	}
	if !passwordReused {
		inSync = false
	}

	backendInSync, reason, err := config.Sync(ctx, obj)
	if err != nil {
		if reason == "" {
			reason = config.SyncErrorReason
		}
		if reason == "" {
			reason = "ReconcileFailed"
		}
		message := err.Error()
		if config.SyncErrorEventText != nil {
			message = config.SyncErrorEventText(obj, reason, err)
		}
		emitEvent(corev1.EventTypeWarning, reason, message)
		statusErr := SetReconcileStatus(
			ctx,
			config.Client,
			req.NamespacedName,
			config.StatusConfig,
			accessv1.ReconcileStateError,
			reason,
			err.Error(),
		)
		if statusErr != nil {
			log.Error(statusErr, "failed to update status after reconcile failure")
		}

		log.Error(err, "failed to reconcile managed access resource", "name", obj.GetName())
		return ctrl.Result{}, err
	}
	if !backendInSync {
		inSync = false
	}

	if !inSync {
		if err := SetReconcileStatus(
			ctx,
			config.Client,
			req.NamespacedName,
			config.StatusConfig,
			accessv1.ReconcileStateInProgress,
			"Reconciling",
			config.InProgressMessage,
		); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: PrivilegeDriftRequeueInterval}, nil
	}

	if err := SetReconcileStatus(
		ctx,
		config.Client,
		req.NamespacedName,
		config.StatusConfig,
		accessv1.ReconcileStateSuccess,
		"Ready",
		config.SuccessMessage,
	); err != nil {
		return ctrl.Result{}, err
	}

	if config.SuccessEventReason != "" && config.SuccessEventMessage != "" {
		emitEvent(corev1.EventTypeNormal, config.SuccessEventReason, config.SuccessEventMessage)
	}

	return ctrl.Result{RequeueAfter: SyncedRequeueInterval}, nil
}

func AddAccessFinalizerIfMissing(ctx context.Context, c client.Client, obj client.Object) error {
	if controllerutil.ContainsFinalizer(obj, AccessResourceFinalizer) {
		return nil
	}

	controllerutil.AddFinalizer(obj, AccessResourceFinalizer)
	return c.Update(ctx, obj)
}

func RemoveAccessFinalizerIfPresent(ctx context.Context, c client.Client, obj client.Object) error {
	if !controllerutil.ContainsFinalizer(obj, AccessResourceFinalizer) {
		return nil
	}

	controllerutil.RemoveFinalizer(obj, AccessResourceFinalizer)
	return c.Update(ctx, obj)
}

func SetReconcileStatus[T client.Object](
	ctx context.Context,
	c client.Client,
	key types.NamespacedName,
	config ReconcileStatusConfig[T],
	reconcileState accessv1.ReconcileState,
	reason,
	message string,
) error {
	if key.Name == "" || key.Namespace == "" {
		return nil
	}

	latest := config.NewObject()
	if err := c.Get(ctx, key, latest); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	readyStatus := metav1.ConditionFalse
	successStatus := metav1.ConditionFalse
	inProgressStatus := metav1.ConditionFalse

	switch reconcileState {
	case accessv1.ReconcileStateSuccess:
		readyStatus = metav1.ConditionTrue
		successStatus = metav1.ConditionTrue
	case accessv1.ReconcileStateInProgress:
		inProgressStatus = metav1.ConditionTrue
	case accessv1.ReconcileStateError:
	default:
		return fmt.Errorf("invalid reconcile state %q", reconcileState)
	}

	conditions := config.Conditions(latest)
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               config.ConditionTypes.Ready,
		Status:             readyStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: latest.GetGeneration(),
	})
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               config.ConditionTypes.Success,
		Status:             successStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: latest.GetGeneration(),
	})
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               config.ConditionTypes.InProgress,
		Status:             inProgressStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: latest.GetGeneration(),
	})
	config.SetLastLog(latest, message)
	config.SetLastReconcileState(latest, reconcileState)

	return c.Status().Update(ctx, latest)
}

func EmitEvent(recorder events.EventRecorder, object client.Object, eventType, reason, message string) {
	if recorder == nil || object == nil {
		return
	}

	message = fmt.Sprintf("%s (at %s)", message, time.Now().Format(time.RFC3339))
	recorder.Eventf(object, nil, eventType, reason, "PolicyValidation", "%s", message)
}
