package controller

import (
	accessv1 "github.com/delta10/access-operator/api/v1"
	rabbithole "github.com/michaelklishin/rabbit-hole/v3"
)

type RabbitMQInterface interface {
	ListUsersAndPermissions() (map[string][]string, error)
	CreateUser(username, password string) error
	DeleteUser(username string) error
	CreateVhost(vhost string) error
	DeleteVhost(vhost string) error
}

func (r *RabbitMQAccessReconciler) ListUsersAndPermissions(
	rmqc *rabbithole.Client,
) (map[string][]accessv1.RabbitMQPermissionSpec, error) {
	usersPermissionsReturn := make(map[string][]accessv1.RabbitMQPermissionSpec)
	usersResp, err := rmqc.ListUsers()
	if err != nil {
		return nil, err
	}

	for _, user := range usersResp {
		permissionsResp, err := rmqc.ListPermissionsOf(user.Name)
		if err != nil {
			return nil, err
		}

		var permissions []accessv1.RabbitMQPermissionSpec
		for _, perm := range permissionsResp {
			permissions = append(permissions, accessv1.RabbitMQPermissionSpec{
				VHost:     perm.Vhost,
				Configure: perm.Configure,
				Write:     perm.Write,
				Read:      perm.Read,
			})
		}
		usersPermissionsReturn[user.Name] = permissions
	}

	return usersPermissionsReturn, nil
}

func (r *RabbitMQAccessReconciler) CreateUser(rmqc *rabbithole.Client, username, password string) error {
	_, err := rmqc.PutUser(username, rabbithole.UserSettings{
		Password: password,
		Tags:     nil,
	})

	return err
}

func (r *RabbitMQAccessReconciler) DeleteUser(rmqc *rabbithole.Client, username string) error {
	_, err := rmqc.DeleteUser(username)

	return err
}

func (r *RabbitMQAccessReconciler) vhostExists(rmqc *rabbithole.Client, vhost string) (bool, error) {
	vhostsResp, err := rmqc.ListVhosts()
	if err != nil {
		return false, err
	}

	for _, vh := range vhostsResp {
		if vh.Name == vhost {
			return true, nil
		}
	}

	return false, nil
}

func (r *RabbitMQAccessReconciler) CreateVhost(rmqc *rabbithole.Client, vhost string) error {
	_, err := rmqc.PutVhost(vhost, rabbithole.VhostSettings{})

	return err
}

func (r *RabbitMQAccessReconciler) DeleteVhost(rmqc *rabbithole.Client, vhost string) error {
	_, err := rmqc.DeleteVhost(vhost)

	return err
}

func (r *RabbitMQAccessReconciler) SetPermissions(rmqc *rabbithole.Client, username string, permissions []accessv1.RabbitMQPermissionSpec) error {
	return r.SetPermissionsExact(rmqc, username, permissions, nil)
}

func (r *RabbitMQAccessReconciler) SetPermissionsExact(
	rmqc *rabbithole.Client,
	username string,
	desiredPermissions []accessv1.RabbitMQPermissionSpec,
	currentPermissions []accessv1.RabbitMQPermissionSpec,
) error {
	for _, vhost := range stalePermissionVHosts(currentPermissions, desiredPermissions) {
		if _, err := rmqc.ClearPermissionsIn(vhost, username); err != nil {
			return err
		}
	}

	for _, perm := range desiredPermissions {
		_, err := rmqc.UpdatePermissionsIn(perm.VHost, username, rabbithole.Permissions{
			Configure: perm.Configure,
			Write:     perm.Write,
			Read:      perm.Read,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// stalePermissionVHosts returns a list of vhosts that are present in currentPermissions but not in desiredPermissions.
// This is used to determine which permissions should be removed.
func stalePermissionVHosts(
	currentPermissions []accessv1.RabbitMQPermissionSpec,
	desiredPermissions []accessv1.RabbitMQPermissionSpec,
) []string {
	desiredVHosts := make(map[string]struct{}, len(desiredPermissions))
	for _, perm := range desiredPermissions {
		desiredVHosts[perm.VHost] = struct{}{}
	}

	stale := make([]string, 0, len(currentPermissions))
	seen := make(map[string]struct{}, len(currentPermissions))
	for _, perm := range currentPermissions {
		if _, keep := desiredVHosts[perm.VHost]; keep {
			continue
		}
		if _, alreadyAdded := seen[perm.VHost]; alreadyAdded {
			continue
		}
		seen[perm.VHost] = struct{}{}
		stale = append(stale, perm.VHost)
	}

	return stale
}
