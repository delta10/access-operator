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

// SecretReference references a key in a Kubernetes Secret
type SecretReference struct {
	// name is the name of the secret
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// key is the key within the secret
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// SecretKeySelector allows selecting a value from a secret or providing a plain string
// +kubebuilder:validation:XValidation:rule="has(self.value) || has(self.secretRef)",message="either value or secretRef must be specified"
type SecretKeySelector struct {
	// value is the plain text value
	// +optional
	Value *string `json:"value,omitempty"`

	// secretRef references a key in a secret
	// +optional
	SecretRef *SecretReference `json:"secretRef,omitempty"`
}

// ConnectionSpec defines connection information for PostgreSQL
type ConnectionSpec struct {
	// existingSecret references an existing secret with connection details
	// The secret must contain keys: host, port, database, username, password
	// Optionally can include: sslmode
	// +optional
	ExistingSecret *string `json:"existingSecret,omitempty"`

	// host is the PostgreSQL server hostname
	// +optional
	// +kubebuilder:validation:MinLength=1
	Host *string `json:"host,omitempty"`

	// port is the PostgreSQL server port
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port *int32 `json:"port,omitempty"`

	// database is the database name
	// +optional
	// +kubebuilder:validation:MinLength=1
	Database *string `json:"database,omitempty"`

	// username for connecting to the database
	// Can be provided as plain text or as a secret reference
	// +optional
	Username *SecretKeySelector `json:"username,omitempty"`

	// password for connecting to the database
	// Can be provided as plain text or as a secret reference
	// +optional
	Password *SecretKeySelector `json:"password,omitempty"`

	// sslMode specifies the SSL mode for the connection
	// Valid values: disable, allow, prefer, require, verify-ca, verify-full
	// Defaults to "require" for security
	// +optional
	// +kubebuilder:default="require"
	// +kubebuilder:validation:Enum=disable;allow;prefer;require;verify-ca;verify-full
	SSLMode *string `json:"sslMode,omitempty"`
}

// GrantSpec defines database grants to be applied
type GrantSpec struct {
	// database is the name of the database to grant privileges on
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Database string `json:"database"`

	// schema is the database schema (defaults to "public")
	// +optional
	// +kubebuilder:default="public"
	// +kubebuilder:validation:MinLength=1
	Schema *string `json:"schema,omitempty"`

	// privileges is the list of privileges to grant
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:Items:Enum=SELECT;INSERT;UPDATE;DELETE;TRUNCATE;REFERENCES;TRIGGER;CREATE;CONNECT;TEMPORARY;EXECUTE;USAGE;SET;ALTER SYSTEM;MAINTAIN
	Privileges []string `json:"privileges"`
}

// PostgresAccessSpec defines the desired state of PostgresAccess
type PostgresAccessSpec struct {
	// generatedSecret is the name of the secret where generated credentials will be stored
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	GeneratedSecret string `json:"generatedSecret"`

	// username is the PostgreSQL username to create
	// If empty, the operator will generate one
	// +optional
	Username *string `json:"username,omitempty"`

	// connection defines how to connect to PostgreSQL
	// Can reference an existing secret or provide connection details directly
	// +kubebuilder:validation:Required
	Connection ConnectionSpec `json:"connection"`

	// grants defines the database privileges to grant
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=1
	Grants []GrantSpec `json:"grants"`
}

// PostgresAccessStatus defines the observed state of PostgresAccess.
type PostgresAccessStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the PostgresAccess resource.
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

// PostgresAccess is the Schema for the postgresaccesses API
type PostgresAccess struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PostgresAccess
	// +required
	Spec PostgresAccessSpec `json:"spec"`

	// status defines the observed state of PostgresAccess
	// +optional
	Status PostgresAccessStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PostgresAccessList contains a list of PostgresAccess
type PostgresAccessList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PostgresAccess `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PostgresAccess{}, &PostgresAccessList{})
}
