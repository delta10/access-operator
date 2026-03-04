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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// VhostCreationPolicy defines if a vhost should be created if its existence is implied in the permissions spec
// +kubebuilder:validation:Enum=Always;IfNotExists;Never
type VhostCreationPolicy string

const (
	VhostCreationPolicyAlways      VhostCreationPolicy = "Always"
	VhostCreationPolicyIfNotExists VhostCreationPolicy = "IfNotExists"
	VhostCreationPolicyNever       VhostCreationPolicy = "Never"
)

type RabbitMQPermissionSpec struct {
	// vhost is the RabbitMQ virtual host to which the permissions apply
	// +kubebuilder:validation:Required
	VHost string `json:"vhost"`

	// configure is the configure permission pattern
	// +kubebuilder:validation:Required
	Configure string `json:"configure"`

	// write is the write permission pattern
	// +kubebuilder:validation:Required
	Write string `json:"write"`

	// read is the read permission pattern
	// +kubebuilder:validation:Required
	Read string `json:"read"`
}

// RabbitMQAccessSpec defines the desired state of RabbitMQAccess
type RabbitMQAccessSpec struct {
	// generatedSecret is the name of the secret where generated credentials will be stored
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	GeneratedSecret string `json:"generatedSecret"`

	// username is the PostgreSQL username to create
	// +kubebuilder:validation:Required
	Username string `json:"username"`

	// how to connect to the RabbitMQ instance, dbname is not needed; this is to keep consistency with postgresaccess
	// +kubebuilder:validation:Required
	Connection ConnectionSpec `json:"connection"`

	// permissions defines the permissions to grant to the user
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Permissions []RabbitMQPermissionSpec `json:"permissions"`

	VhostCreationPolicy *VhostCreationPolicy `json:"vhostCreationPolicy,omitempty"`
}

// RabbitMQAccessStatus defines the observed state of RabbitMQAccess.
type RabbitMQAccessStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the RabbitMQAccess resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// RabbitMQAccess is the Schema for the rabbitmqaccesses API
type RabbitMQAccess struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of RabbitMQAccess
	// +required
	Spec RabbitMQAccessSpec `json:"spec"`

	// status defines the observed state of RabbitMQAccess
	// +optional
	Status RabbitMQAccessStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// RabbitMQAccessList contains a list of RabbitMQAccess
type RabbitMQAccessList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []RabbitMQAccess `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RabbitMQAccess{}, &RabbitMQAccessList{})
}
