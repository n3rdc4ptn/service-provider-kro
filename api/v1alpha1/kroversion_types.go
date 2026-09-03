/*
Copyright 2025.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// KroVersionRequestSpec defines the desired kro version a user wants installed
// in their kcp workspace.
type KroVersionRequestSpec struct {
	// Version is the kro version the user wants installed.
	// Must match one of the versions offered in the ProviderConfig.
	// +required
	Version string `json:"version"`
}

// KroVersionRequestPhase represents the lifecycle phase of a KroVersionRequest.
type KroVersionRequestPhase string

// KroVersionRequestPhase constants represent the valid lifecycle phases of a KroVersionRequest.
const (
	KroVersionRequestPhasePending      KroVersionRequestPhase = "Pending"
	KroVersionRequestPhaseProvisioning KroVersionRequestPhase = "Provisioning"
	KroVersionRequestPhaseReady        KroVersionRequestPhase = "Ready"
	KroVersionRequestPhaseFailed       KroVersionRequestPhase = "Failed"
	KroVersionRequestPhaseTerminating  KroVersionRequestPhase = "Terminating"
)

// KroVersionRequestStatus defines the observed state of a KroVersionRequest.
type KroVersionRequestStatus struct {
	// Phase is the current lifecycle phase.
	// +optional
	Phase KroVersionRequestPhase `json:"phase,omitempty"`

	// Message provides human-readable details about the current phase.
	// +optional
	Message string `json:"message,omitempty"`

	// InstalledVersion is the version that is currently installed.
	// +optional
	InstalledVersion string `json:"installedVersion,omitempty"`

	// AvailableVersions lists the versions the provider currently offers.
	// +optional
	AvailableVersions []string `json:"availableVersions,omitempty"`

	// Conditions represent the current state of the resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// KroVersionRequest is the user-facing API exposed via kcp APIExport.
// Users create this cluster-scoped resource in their workspace to request
// a kro installation of a specific version.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:JSONPath=`.spec.version`,name="Version",type=string
// +kubebuilder:printcolumn:JSONPath=`.status.phase`,name="Phase",type=string
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type KroVersionRequest struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired kro version
	// +required
	Spec KroVersionRequestSpec `json:"spec"`

	// status defines the observed state
	// +optional
	Status KroVersionRequestStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// KroVersionRequestList contains a list of KroVersionRequest
type KroVersionRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KroVersionRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &KroVersionRequest{}, &KroVersionRequestList{})
		return nil
	})
}
