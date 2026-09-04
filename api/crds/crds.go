package crds

import (
	"embed"

	crdutil "github.com/openmcp-project/controller-utils/pkg/crds"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

//go:embed manifests

// CRDFS holds the embedded CRD manifest files.
var CRDFS embed.FS

// CRDs returns all CustomResourceDefinitions embedded in the manifests directory.
func CRDs() ([]*apiextv1.CustomResourceDefinition, error) {
	return crdutil.CRDsFromFileSystem(CRDFS, "manifests")
}
