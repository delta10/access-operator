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

package v1

// StaleUserDeletionPolicy defines how the controller handles managed users
// that are no longer referenced by any managed access resource.
// +kubebuilder:validation:Enum=Delete;Restrict
type StaleUserDeletionPolicy string

const (
	// StaleUserDeletionPolicyDelete removes unreferenced managed users.
	StaleUserDeletionPolicyDelete StaleUserDeletionPolicy = "Delete"
	// StaleUserDeletionPolicyRestrict retains unreferenced managed users.
	StaleUserDeletionPolicyRestrict StaleUserDeletionPolicy = "Restrict"
)

// StaleVhostDeletionPolicy defines how the controller handles RabbitMQ vhosts
// that are no longer referenced by any managed RabbitMQAccess resource.
// +kubebuilder:validation:Enum=Delete;Retain
type StaleVhostDeletionPolicy string

const (
	// StaleVhostDeletionPolicyDelete removes unreferenced RabbitMQ vhosts.
	StaleVhostDeletionPolicyDelete StaleVhostDeletionPolicy = "Delete"
	// StaleVhostDeletionPolicyRetain leaves unreferenced RabbitMQ vhosts intact.
	StaleVhostDeletionPolicyRetain StaleVhostDeletionPolicy = "Retain"
)

type RabbitMQControllerSettings struct {
	// excludedUsers is a list of RabbitMQ usernames that the controller ignores
	// when reconciling RabbitMQAccess resources.
	// This prevents the operator from creating, updating, or deleting the listed users.
	// +listType=set
	// +optional
	ExcludedUsers []string `json:"excludedUsers,omitempty"`

	// excludedVhosts is a list of RabbitMQ vhosts that the controller ignores
	// when reconciling stale RabbitMQ vhosts.
	// This prevents the operator from deleting the listed vhosts.
	// +listType=set
	// +optional
	ExcludedVhosts []string `json:"excludedVhosts,omitempty"`

	// staleVhostDeletionPolicy controls whether the controller deletes RabbitMQ
	// vhosts that are no longer referenced by any managed RabbitMQAccess.
	// +optional
	// +kubebuilder:default="Retain"
	StaleVhostDeletionPolicy *StaleVhostDeletionPolicy `json:"staleVhostDeletionPolicy,omitempty"`

	// staleUserDeletionPolicy controls whether the controller deletes RabbitMQ
	// users that are no longer referenced by any managed RabbitMQAccess.
	// Restrict retains stale users instead of deleting them.
	// +optional
	// +kubebuilder:default="Restrict"
	StaleUserDeletionPolicy *StaleUserDeletionPolicy `json:"staleUserDeletionPolicy,omitempty"`
}

type PostgresControllerSettings struct {
	// excludedUsers is a list of PostgreSQL usernames that the controller ignores
	// when reconciling PostgresAccess resources.
	// This prevents the operator from creating, updating, or deleting the listed roles.
	// +listType=set
	// +optional
	ExcludedUsers []string `json:"excludedUsers,omitempty"`

	// staleUserDeletionPolicy controls whether the controller deletes PostgreSQL
	// roles that are no longer referenced by any managed PostgresAccess.
	// Restrict retains stale roles, while Retain only permits deletion during
	// finalization of the specific PostgresAccess being removed.
	// +optional
	// +kubebuilder:default="Restrict"
	StaleUserDeletionPolicy *PostgresCleanupPolicy `json:"staleUserDeletionPolicy,omitempty"`
}

type RedisControllerSettings struct {
	// excludedUsers is a list of Redis ACL usernames that the controller ignores
	// when reconciling RedisAccess resources.
	// This prevents the operator from creating, updating, or deleting the listed users.
	// +listType=set
	// +optional
	ExcludedUsers []string `json:"excludedUsers,omitempty"`

	// staleUserDeletionPolicy controls whether the controller deletes Redis ACL
	// users that are no longer referenced by any managed RedisAccess.
	// Restrict retains stale users instead of deleting them.
	// +optional
	// +kubebuilder:default="Restrict"
	StaleUserDeletionPolicy *StaleUserDeletionPolicy `json:"staleUserDeletionPolicy,omitempty"`
}

// ControllerSettings defines operator-wide behavior toggles.
type ControllerSettings struct {
	// existingSecretNamespace enables cross-namespace references for
	// managed access resources that use spec.connection.existingSecretNamespace.
	// +optional
	// +kubebuilder:default=false
	ExistingSecretNamespace bool `json:"existingSecretNamespace,omitempty"`

	// postgres contains settings specific to PostgresAccess controllers.
	// +optional
	PostgresSettings PostgresControllerSettings `json:"postgres,omitempty"`

	// rabbitmq contains settings specific to RabbitMQAccess controllers.
	// +optional
	RabbitMQSettings RabbitMQControllerSettings `json:"rabbitmq,omitempty"`

	// redis contains settings specific to RedisAccess controllers.
	// +optional
	RedisSettings RedisControllerSettings `json:"redis,omitempty"`
}

// ControllerSpec defines config document structure for operator settings.
type ControllerSpec struct {
	// settings contains operator-wide settings.
	// +optional
	Settings ControllerSettings `json:"settings,omitempty"`
}
