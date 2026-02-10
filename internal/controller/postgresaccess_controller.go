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

	"math/big"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"crypto/rand"

	accessv1 "github.com/delta10/access-operator/api/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// PostgresAccessReconciler reconciles a PostgresAccess object
type PostgresAccessReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	DB     DBInterface
}

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=postgresaccesses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=postgresaccesses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=access.k8s.delta10.nl,resources=postgresaccesses/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *PostgresAccessReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var pg accessv1.PostgresAccess
	if err := r.Get(ctx, req.NamespacedName, &pg); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	key := types.NamespacedName{
		Name:      pg.Spec.GeneratedSecret,
		Namespace: req.Namespace,
	}

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
	}

	var username string
	var password string

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sec, func() error {
		sec.Type = corev1.SecretTypeOpaque

		if sec.Data == nil {
			sec.Data = map[string][]byte{}
		}
		// Keep credentials stable across reconciles to avoid update loops.
		if pg.Spec.Username != nil && *pg.Spec.Username != "" {
			username = *pg.Spec.Username
		} else if existingUsername, ok := sec.Data["username"]; ok && len(existingUsername) > 0 {
			username = string(existingUsername)
		} else {
			username = generateUsername(pg.Name)
		}

		if existingPassword, ok := sec.Data["password"]; ok && len(existingPassword) > 0 {
			password = string(existingPassword)
		} else {
			password = rand.Text()
		}

		sec.Data["username"] = []byte(username)
		sec.Data["password"] = []byte(password)

		return controllerutil.SetControllerReference(&pg, sec, r.Scheme)
	})
	if err != nil {
		log.Error(err, "failed to create/update secret", "secret", key.String())
		return ctrl.Result{}, err
	}

	if pg.Spec.Connection.Host != nil && *pg.Spec.Connection.Host != "" &&
		pg.Spec.Connection.Port != nil &&
		pg.Spec.Connection.Database != nil && *pg.Spec.Connection.Database != "" {

		adminUser := "fay"
		adminPassword := "fay"
		if pg.Spec.Connection.Username != nil && pg.Spec.Connection.Username.Value != nil {
			adminUser = *pg.Spec.Connection.Username.Value
		}
		if pg.Spec.Connection.Password != nil && pg.Spec.Connection.Password.Value != nil {
			adminPassword = *pg.Spec.Connection.Password.Value
		}
		connectionString := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s",
			adminUser,
			adminPassword,
			*pg.Spec.Connection.Host,
			*pg.Spec.Connection.Port,
			*pg.Spec.Connection.Database,
		)

		if r.DB == nil {
			r.DB = NewPostgresDB()
		}

		err := r.DB.Connect(ctx, connectionString)
		if err != nil {
			log.Error(err, "Unable to connect to database", "connectionString", connectionString)
			return ctrl.Result{}, err
		}
		defer r.DB.Close(ctx)

		// create user and grant privileges
		err = r.DB.CreateUser(ctx, username, password)
		if err != nil {
			log.Error(err, "failed to create user in PostgreSQL", "username", username)
			return ctrl.Result{}, err
		}

		err = r.DB.GrantPrivileges(ctx, pg.Spec.Grants, username)
		if err != nil {
			log.Error(err, "failed to grant privileges in PostgreSQL", "username", username)
			return ctrl.Result{}, err
		}
	} else if pg.Spec.Connection.ExistingSecret != nil && *pg.Spec.Connection.ExistingSecret != "" {
		log.Info("connection details provided via existing secret, skipping user creation and grants")
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresAccessReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&accessv1.PostgresAccess{}).
		Named("postgresaccess").
		Owns(&corev1.Secret{}).
		Complete(r)
}

func generateUsername(resourceName string) string {
	number, err := rand.Int(rand.Reader, big.NewInt(999999))
	if err != nil {
		return ""
	}

	numberString := fmt.Sprintf("%06d", number.Int64())
	return fmt.Sprintf("%s-%s", resourceName, numberString)
}
