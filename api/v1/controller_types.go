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

// ControllerSettings defines operator-wide behavior toggles.
type ControllerSettings struct {
	// existingSecretNamespace enables cross-namespace references for
	// PostgresAccess.spec.connection.existingSecretNamespace.
	// +optional
	// +kubebuilder:default=false
	ExistingSecretNamespace bool `json:"existingSecretNamespace,omitempty"`
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
