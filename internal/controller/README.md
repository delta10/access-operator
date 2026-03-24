# Adding a controller

This directory contains the reconciliation logic for the operator.

Current patterns in this repository:

- `internal/controller/controller_controller.go` contains operator-wide reconciliation for the `Controller` CR.
- `internal/controller/postgres/` contains the `PostgresAccess` controller and PostgreSQL-specific helpers.
- `internal/controller/rabbitMQ/` contains the `RabbitMQAccess` controller and RabbitMQ-specific helpers.
- `internal/controller/redis/` contains the `RedisAccess` controller and Redis ACL-specific helpers.
- `internal/controller/shared_*_logic.go` contains shared helpers for access-style controllers that generate credentials, resolve shared config, and reconcile an external backend.

Use this guide when adding a new controller under `internal/controller`.

## 1. Decide which kind of controller you are adding

Use a backend-specific subpackage such as `internal/controller/myservice/` when the controller manages one external system and needs its own helpers, tests, or client wrappers.

Keep the controller in the root `internal/controller/` package only when it is truly shared or operator-wide, similar to [`controller_controller.go`](./controller_controller.go).

## 2. Scaffold the API with Kubebuilder

Always scaffold with Kubebuilder first.

For a new CRD managed by this operator:

```bash
kubebuilder create api --group access --version v1 --kind MyServiceAccess --resource=true --controller=true
```

After scaffolding:

- Keep the API type in `api/v1/`.
- Do not edit generated files such as `zz_generated.deepcopy.go`, `config/crd/bases/*`, or `config/rbac/role.yaml` by hand.
- Do not remove Kubebuilder scaffold markers from generated files.

## 3. Define the API shape

Implement the new resource in `api/v1/*_types.go`.

Follow the existing `PostgresAccess` and `RabbitMQAccess` pattern:

- Put desired state in `Spec`.
- Put `[]metav1.Condition` in `Status.Conditions`.
- Track reconcile progress with `Status.LastLog` and `Status.LastReconcileState`.
- Prefer standard Kubernetes types such as `metav1.Time` and `metav1.Condition`.
- Add validation and defaults with Kubebuilder markers instead of custom parsing where possible.

If you change markers or fields in `*_types.go`, regenerate artifacts:

```bash
make manifests
make generate
```

## 4. Organize the controller package

Scaffold with Kubebuilder first, then organize backend-specific implementation under `internal/controller/<name>/` when the controller needs its own helpers and tests.

Typical files for a new backend controller:

- `<kind>_controller.go` for the reconciler and `SetupWithManager`
- helper files for connection or backend-specific logic
- `<kind>_controller_test.go` for unit tests
- `suite_test.go` only if the package needs its own envtest suite

## 5. Reuse the shared managed-access flow when it fits

If the new controller:

- manages a CR owned by this operator
- creates or maintains a generated credentials secret
- reconciles an external backend
- uses the standard status model

then prefer `controller.ReconcileManagedAccess` from [`shared_reconciliation_logic.go`](./shared_reconciliation_logic.go).

The existing examples are:

- [`internal/controller/postgres/postgresaccess_controller.go`](./postgres/postgresaccess_controller.go)
- [`internal/controller/rabbitMQ/rabbitmqaccess_controller.go`](./rabbitMQ/rabbitmqaccess_controller.go)

Minimal shape:

```go
func myServiceReconcileStatusConfig() controller.ReconcileStatusConfig[*accessv1.MyServiceAccess] {
	return controller.NewStandardReconcileStatusConfig(
		func() *accessv1.MyServiceAccess { return &accessv1.MyServiceAccess{} },
		func(obj *accessv1.MyServiceAccess) *[]metav1.Condition { return &obj.Status.Conditions },
		func(obj *accessv1.MyServiceAccess, message string) { obj.Status.LastLog = message },
		func(obj *accessv1.MyServiceAccess, state accessv1.ReconcileState) {
			obj.Status.LastReconcileState = state
		},
	)
}

func (r *MyServiceAccessReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return controller.ReconcileManagedAccess(ctx, req, controller.ManagedAccessReconcileConfig[*accessv1.MyServiceAccess]{
		Client:       r.Client,
		Scheme:       r.Scheme,
		StatusConfig: myServiceReconcileStatusConfig(),
		Finalize:     r.finalizeMyServiceAccess,
		Sync:         r.reconcileMyServiceAccess,
		SecretName: func(obj *accessv1.MyServiceAccess) string { return obj.Spec.GeneratedSecret },
		Username:   func(obj *accessv1.MyServiceAccess) string { return obj.Spec.Username },
		EmitEvent:  func(obj *accessv1.MyServiceAccess, eventType, reason, message string) { controller.EmitEvent(r.Recorder, obj, eventType, reason, message) },
	})
}

func (r *MyServiceAccessReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&accessv1.MyServiceAccess{}).
		Owns(&corev1.Secret{}).
		Named("myserviceaccess").
		Complete(r)
}
```

Use a custom reconcile loop instead of `ReconcileManagedAccess` when the controller is not access-oriented, does not manage generated credentials, or needs different status or watch behavior. The `ControllerReconciler` in [`controller_controller.go`](./controller_controller.go) is the example for that shape.

## 6. Implement reconciliation with repository conventions

Match the conventions already used in this repository:

- Add RBAC markers in the controller file for every resource the reconciler reads or writes.
- Use structured logging via `logf.FromContext(ctx)`.
- Keep reconciliation idempotent.
- Re-fetch or use `CreateOrUpdate` before mutating cluster state when conflicts are possible.
- Use the shared finalizer constant `controller.AccessResourceFinalizer` for managed access resources.
- Watch secondary resources with `.Owns(...)` or `.Watches(...)` instead of relying only on timed requeues.
- Emit Kubernetes events for success and failure paths when the resource is user-facing.

## 7. Register the controller in the manager

Wire the controller in `cmd/main.go`:

1. Add the package import.
2. Instantiate the reconciler.
3. Call `SetupWithManager(mgr)`.

Follow the existing blocks for Postgres and RabbitMQ. Keep the `// +kubebuilder:scaffold:*` markers intact.

## 8. Add tests

Every significant controller change should come with tests.

Use the existing packages as templates:

- [`internal/controller/postgres/postgresaccess_controller_test.go`](./postgres/postgresaccess_controller_test.go)
- [`internal/controller/rabbitMQ/rabbitmqaccess_controller_test.go`](./rabbitMQ/rabbitmqaccess_controller_test.go)
- [`test/e2e/postgres_e2e_test.go`](../../test/e2e/postgres_e2e_test.go)
- [`test/e2e/rabbitmq_e2e_test.go`](../../test/e2e/rabbitmq_e2e_test.go)

Recommended coverage:

- reconcile success
- validation or backend failure paths
- finalizer behavior
- generated secret behavior if the controller owns credentials
- e2e coverage for major user-facing behavior

## 9. Add a sample manifest

Add or update a sample in `config/samples/` so the resource can be exercised quickly in a cluster.

## 10. Run the required commands

After changing API markers or types:

```bash
make manifests
make generate
```

After changing Go code:

```bash
make lint-fix
make test
```

For major features, also run:

```bash
make test-e2e
```

## Checklist

Before opening a PR, confirm that:

- the API type is defined in `api/v1/`
- generated files were refreshed with `make manifests` and `make generate`
- the controller is registered in `cmd/main.go`
- RBAC markers cover all accessed resources
- tests were added or updated
- a sample manifest exists in `config/samples/`
