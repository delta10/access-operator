package redis

import (
	"context"
	"fmt"

	accessv1 "github.com/delta10/access-operator/api/v1"
	"github.com/delta10/access-operator/internal/controller"
)

var connectionDefaults = accessv1.ConnectionSpec{}

func (r *RedisAccessReconciler) getConnectionDetails(
	ctx context.Context,
	redisAccess *accessv1.RedisAccess,
) (controller.ConnectionDetails, error) {
	if redisAccess.Spec.Connection.ExistingSecret != nil && *redisAccess.Spec.Connection.ExistingSecret != "" {
		secretNamespace, err := r.resolveExistingSecretNamespace(ctx, redisAccess)
		if err != nil {
			return controller.ConnectionDetails{}, err
		}

		return controller.GetExistingSecretConnectionDetails(
			ctx,
			r.Client,
			*redisAccess.Spec.Connection.ExistingSecret,
			secretNamespace,
			&redisAccess.Spec.Connection,
			connectionDefaults,
		)
	}

	if controller.HasDirectConnectionDetails(redisAccess.Spec.Connection) {
		return controller.GetDirectConnectionDetails(
			ctx,
			r.Client,
			redisAccess.Spec.Connection,
			redisAccess.Namespace,
			connectionDefaults,
		)
	}

	return controller.ConnectionDetails{}, fmt.Errorf("no valid connection details provided")
}

func (r *RedisAccessReconciler) resolveExistingSecretNamespace(
	ctx context.Context,
	redisAccess *accessv1.RedisAccess,
) (string, error) {
	secretNamespace, err := controller.ResolveConnectionSecretNamespace(
		ctx,
		r.Client,
		redisAccess.Namespace,
		redisAccess.Spec.Connection.ExistingSecretNamespace,
	)
	if err != nil {
		return "", err
	}

	return secretNamespace, nil
}

func (r *RedisAccessReconciler) resolveExcludedUsers(ctx context.Context) (map[string]struct{}, error) {
	settings, err := resolveRedisControllerSettings(ctx, r)
	if err != nil {
		return nil, err
	}

	return controller.NormalizeExcludedUsers(settings.RedisSettings.ExcludedUsers), nil
}

func (r *RedisAccessReconciler) resolveStaleUserDeletionPolicy(ctx context.Context) (accessv1.StaleUserDeletionPolicy, error) {
	if r.Client == nil {
		return accessv1.StaleUserDeletionPolicyRestrict, nil
	}

	settings, err := resolveRedisControllerSettings(ctx, r)
	if err != nil {
		return "", err
	}

	if settings.RedisSettings.StaleUserDeletionPolicy == nil {
		return accessv1.StaleUserDeletionPolicyRestrict, nil
	}

	return *settings.RedisSettings.StaleUserDeletionPolicy, nil
}

func resolveRedisControllerSettings(ctx context.Context, r *RedisAccessReconciler) (accessv1.ControllerSettings, error) {
	return controller.ResolveControllerSettings(ctx, r.Client)
}
