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
	"k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto copies all properties of this object into another object of the same type.
func (in *Controller) DeepCopyInto(out *Controller) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy returns a deep copy of this object.
func (in *Controller) DeepCopy() *Controller {
	if in == nil {
		return nil
	}
	out := new(Controller)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject returns a generically typed copy of this object.
func (in *Controller) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto copies all properties of this object into another object of the same type.
func (in *ControllerList) DeepCopyInto(out *ControllerList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]Controller, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy returns a deep copy of this object.
func (in *ControllerList) DeepCopy() *ControllerList {
	if in == nil {
		return nil
	}
	out := new(ControllerList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject returns a generically typed copy of this object.
func (in *ControllerList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto copies all properties of this object into another object of the same type.
func (in *ControllerSettings) DeepCopyInto(out *ControllerSettings) {
	*out = *in
	in.PostgresSettings.DeepCopyInto(&out.PostgresSettings)
}

// DeepCopy returns a deep copy of this object.
func (in *ControllerSettings) DeepCopy() *ControllerSettings {
	if in == nil {
		return nil
	}
	out := new(ControllerSettings)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies all properties of this object into another object of the same type.
func (in *ControllerSpec) DeepCopyInto(out *ControllerSpec) {
	*out = *in
	in.Settings.DeepCopyInto(&out.Settings)
}

// DeepCopyInto copies all properties of this object into another object of the same type.
func (in *PostgresControllerSettings) DeepCopyInto(out *PostgresControllerSettings) {
	*out = *in
	if in.ExcludedUsers != nil {
		in, out := &in.ExcludedUsers, &out.ExcludedUsers
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
}

// DeepCopy returns a deep copy of this object.
func (in *PostgresControllerSettings) DeepCopy() *PostgresControllerSettings {
	if in == nil {
		return nil
	}
	out := new(PostgresControllerSettings)
	in.DeepCopyInto(out)
	return out
}

// DeepCopy returns a deep copy of this object.
func (in *ControllerSpec) DeepCopy() *ControllerSpec {
	if in == nil {
		return nil
	}
	out := new(ControllerSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies all properties of this object into another object of the same type.
func (in *ControllerStatus) DeepCopyInto(out *ControllerStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy returns a deep copy of this object.
func (in *ControllerStatus) DeepCopy() *ControllerStatus {
	if in == nil {
		return nil
	}
	out := new(ControllerStatus)
	in.DeepCopyInto(out)
	return out
}
