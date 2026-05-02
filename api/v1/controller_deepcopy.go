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

func (in *ControllerSettings) DeepCopyInto(out *ControllerSettings) {
	*out = *in
	in.PostgresSettings.DeepCopyInto(&out.PostgresSettings)
	in.RabbitMQSettings.DeepCopyInto(&out.RabbitMQSettings)
	in.RedisSettings.DeepCopyInto(&out.RedisSettings)
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
func (in *PostgresControllerSettings) DeepCopyInto(out *PostgresControllerSettings) {
	*out = *in
	if in.ExcludedUsers != nil {
		in, out := &in.ExcludedUsers, &out.ExcludedUsers
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	if in.StaleUserDeletionPolicy != nil {
		in, out := &in.StaleUserDeletionPolicy, &out.StaleUserDeletionPolicy
		*out = new(PostgresCleanupPolicy)
		**out = **in
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
