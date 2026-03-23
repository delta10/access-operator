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

// RedisAccessSpec defines the desired state of RedisAccess.
type RedisAccessSpec struct {
	// generatedSecret is the name of the secret where generated credentials will be stored.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	GeneratedSecret string `json:"generatedSecret"`

	// username is the Redis ACL username to create.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Username string `json:"username"`

	// connection defines how to connect to Redis.
	// Can reference an existing secret or provide connection details directly.
	// +kubebuilder:validation:Required
	Connection ConnectionSpec `json:"connection"`

	// aclRules defines the Redis ACL directives applied after reset, on, and the managed password.
	// Order is preserved because Redis ACL directives are order-sensitive.
	// Controller-owned authentication directives such as reset, on/off, and password rules are rejected.
	// +listType=atomic
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:items:MinLength=1
	ACLRules []string `json:"aclRules"`
}

// RedisAccessStatus defines the observed state of RedisAccess.
type RedisAccessStatus struct {
	// conditions represent the current state of the RedisAccess resource.
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

	// lastLog is the latest reconcile log message or error.
	// +optional
	LastLog string `json:"lastLog,omitempty"`

	// lastReconcileState is the latest reconcile outcome.
	// +kubebuilder:default="InProgress"
	// +optional
	LastReconcileState ReconcileState `json:"lastReconcileState,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=".status.lastReconcileState"
// +kubebuilder:printcolumn:name="LastLog",type=string,JSONPath=".status.lastLog"

// RedisAccess is the Schema for the redisaccesses API.
type RedisAccess struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of RedisAccess
	// +required
	Spec RedisAccessSpec `json:"spec"`

	// status defines the observed state of RedisAccess
	// +optional
	Status RedisAccessStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// RedisAccessList contains a list of RedisAccess.
type RedisAccessList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []RedisAccess `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RedisAccess{}, &RedisAccessList{})
}
