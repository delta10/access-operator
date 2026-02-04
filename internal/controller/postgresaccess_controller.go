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
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	podsk8sdelta10nlv1 "github.com/delta10/access-operator/api/v1"
)

// PostgresAccessReconciler reconciles a PostgresAccess object
type PostgresAccessReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

var once sync.Once

// +kubebuilder:rbac:groups=pods.k8s.delta10.nl.k8s.delta10.nl,resources=postgresaccesses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pods.k8s.delta10.nl.k8s.delta10.nl,resources=postgresaccesses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pods.k8s.delta10.nl.k8s.delta10.nl,resources=postgresaccesses/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the PostgresAccess object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
func (r *PostgresAccessReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)
	_ = req

	once.Do(func() {
		var postgresAccessList podsk8sdelta10nlv1.PostgresAccessList
		if err := r.List(ctx, &postgresAccessList); err != nil {
			fmt.Printf("Error listing PostgresAccess resources: %v\n", err)
			return
		}
		for _, item := range postgresAccessList.Items {
			fmt.Printf("PostgresAccess: %s\n", item.Name)
		}
	})

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresAccessReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&podsk8sdelta10nlv1.PostgresAccess{}).
		Named("postgresaccess").
		Complete(r)
}
