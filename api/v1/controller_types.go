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

// +kubebuilder:object:generate=true
package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
}

type PostgresControllerSettings struct {
	// excludedUsers is a list of PostgreSQL usernames that the controller ignores
	// when reconciling PostgresAccess resources.
	// This prevents the operator from creating, updating, or deleting the listed roles.
	// +listType=set
	// +optional
	ExcludedUsers []string `json:"excludedUsers,omitempty"`
}

type RedisControllerSettings struct {
	// excludedUsers is a list of Redis ACL usernames that the controller ignores
	// when reconciling RedisAccess resources.
	// This prevents the operator from creating, updating, or deleting the listed users.
	// +listType=set
	// +optional
	ExcludedUsers []string `json:"excludedUsers,omitempty"`
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

// ControllerSpec defines the desired state of Controller.
type ControllerSpec struct {
	// settings contains operator-wide settings.
	// +optional
	Settings ControllerSettings `json:"settings,omitempty"`
}

// ControllerStatus defines the observed state of Controller.
type ControllerStatus struct {
	// conditions represent the current state of this Controller resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:path=controllers,scope=Namespaced,singular=controller,shortName=actrl

// Controller is the Schema for the controllers API.
type Controller struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Controller.
	// +optional
	Spec ControllerSpec `json:"spec,omitzero"`

	// status defines the observed state of Controller.
	// +optional
	Status ControllerStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ControllerList contains a list of Controller.
type ControllerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Controller `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Controller{}, &ControllerList{})
}
