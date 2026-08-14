package kcp

import (
	"context"
	"encoding/json"
	"fmt"

	apisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/sdk/apis/apis/v1alpha2"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	apiv1alpha1 "github.com/openmcp-project/service-provider-kro/api/v1alpha1"
)

const (
	// APIExportName is the single APIExport name for the KroVersionRequest resource.
	APIExportName = "kro-service"

	// schemaName is the APIResourceSchema name for KroVersionRequest.
	// kcp requires the format: <prefix>.<plural>.<group>
	schemaName = "v1alpha1.kroversionrequests.kro.services.open-control-plane.io"
)

// Provisioner ensures the APIResourceSchema and APIExport for KroVersionRequest
// exist in the kcp provider workspace.
type Provisioner struct {
	kcpClient client.Client
}

// NewProvisioner creates a Provisioner.
func NewProvisioner(kcpClient client.Client) *Provisioner {
	return &Provisioner{kcpClient: kcpClient}
}

// Reconcile ensures the APIResourceSchema and APIExport for KroVersionRequest exist.
func (p *Provisioner) Reconcile(ctx context.Context, _ *apiv1alpha1.ProviderConfig) error {
	l := logf.FromContext(ctx)

	schema, err := kroVersionRequestSchema()
	if err != nil {
		return fmt.Errorf("building APIResourceSchema: %w", err)
	}
	if err := p.ensureSchema(ctx, schema); err != nil {
		return fmt.Errorf("ensuring APIResourceSchema: %w", err)
	}

	export := kroVersionRequestAPIExport()
	if err := p.ensureAPIExport(ctx, export); err != nil {
		return fmt.Errorf("ensuring APIExport: %w", err)
	}

	l.Info("kcp API provisioning complete", "export", APIExportName)
	return nil
}

// kroVersionRequestSchema builds the APIResourceSchema for the KroVersionRequest CRD.
func kroVersionRequestSchema() (*apisv1alpha1.APIResourceSchema, error) {
	// Build the OpenAPI v3 schema as JSON
	schemaProps := &apiextensionsv1.JSONSchemaProps{
		Type:        "object",
		Description: "KroVersionRequest defines the desired kro version for this workspace",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type:        "object",
				Description: "spec defines the desired kro version",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"version": {
						Type:        "string",
						Description: "Version is the kro version to install",
					},
				},
				Required: []string{"version"},
			},
			"status": {
				Type:        "object",
				Description: "status defines the observed state",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"phase": {
						Type:        "string",
						Description: "Phase is the current lifecycle phase",
					},
					"message": {
						Type:        "string",
						Description: "Human-readable details about the current phase",
					},
					"installedVersion": {
						Type:        "string",
						Description: "The version currently installed",
					},
					"availableVersions": {
						Type:        "array",
						Description: "Versions the provider currently offers",
						Items: &apiextensionsv1.JSONSchemaPropsOrArray{
							Schema: &apiextensionsv1.JSONSchemaProps{Type: "string"},
						},
					},
					"conditions": {
						Type:        "array",
						Description: "Conditions represent the current state",
						Items: &apiextensionsv1.JSONSchemaPropsOrArray{
							Schema: &apiextensionsv1.JSONSchemaProps{
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"type":               {Type: "string"},
									"status":             {Type: "string"},
									"reason":             {Type: "string"},
									"message":            {Type: "string"},
									"lastTransitionTime": {Type: "string", Format: "date-time"},
								},
								Required: []string{"type", "status", "reason", "lastTransitionTime"},
							},
						},
					},
				},
			},
		},
		Required: []string{"spec"},
	}

	raw, err := json.Marshal(schemaProps)
	if err != nil {
		return nil, fmt.Errorf("marshaling schema: %w", err)
	}

	return &apisv1alpha1.APIResourceSchema{
		ObjectMeta: metav1.ObjectMeta{
			Name: schemaName,
		},
		Spec: apisv1alpha1.APIResourceSchemaSpec{
			Group: apiv1alpha1.GroupVersion.Group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind:     "KroVersionRequest",
				ListKind: "KroVersionRequestList",
				Plural:   "kroversionrequests",
				Singular: "kroversionrequest",
			},
			Scope: apiextensionsv1.ClusterScoped,
			Versions: []apisv1alpha1.APIResourceVersion{
				{
					Name:    "v1alpha1",
					Served:  true,
					Storage: true,
					Schema:  runtime.RawExtension{Raw: raw},
					Subresources: apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
					AdditionalPrinterColumns: []apiextensionsv1.CustomResourceColumnDefinition{
						{Name: "Version", Type: "string", JSONPath: ".spec.version"},
						{Name: "Phase", Type: "string", JSONPath: ".status.phase"},
						{Name: "Age", Type: "date", JSONPath: ".metadata.creationTimestamp"},
					},
				},
			},
		},
	}, nil
}

// kroVersionRequestAPIExport builds the APIExport for the KroVersionRequest service.
func kroVersionRequestAPIExport() *apisv1alpha2.APIExport {
	return &apisv1alpha2.APIExport{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apis.kcp.io/v1alpha2",
			Kind:       "APIExport",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: APIExportName,
		},
		Spec: apisv1alpha2.APIExportSpec{
			Resources: []apisv1alpha2.ResourceSchema{
				{
					Group:  apiv1alpha1.GroupVersion.Group,
					Name:   "kroversionrequests",
					Schema: schemaName,
					Storage: apisv1alpha2.ResourceSchemaStorage{
						CRD: &apisv1alpha2.ResourceSchemaStorageCRD{},
					},
				},
			},
			PermissionClaims: []apisv1alpha2.PermissionClaim{
				{
					GroupResource: apisv1alpha2.GroupResource{Group: "", Resource: "serviceaccounts"},
					Verbs:         []string{"*"},
				},
				{
					GroupResource: apisv1alpha2.GroupResource{Group: "rbac.authorization.k8s.io", Resource: "clusterroles"},
					Verbs:         []string{"*"},
				},
				{
					GroupResource: apisv1alpha2.GroupResource{Group: "rbac.authorization.k8s.io", Resource: "clusterrolebindings"},
					Verbs:         []string{"*"},
				},
			},
		},
	}
}

// ensureSchema creates an APIResourceSchema if it doesn't exist.
func (p *Provisioner) ensureSchema(ctx context.Context, schema *apisv1alpha1.APIResourceSchema) error {
	existing := &apisv1alpha1.APIResourceSchema{}
	err := p.kcpClient.Get(ctx, client.ObjectKeyFromObject(schema), existing)
	if err == nil {
		return nil // immutable, skip
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("checking schema %s: %w", schema.Name, err)
	}
	if err := p.kcpClient.Create(ctx, schema); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("creating schema %s: %w", schema.Name, err)
	}
	return nil
}

// ensureAPIExport creates or updates an APIExport.
func (p *Provisioner) ensureAPIExport(ctx context.Context, export *apisv1alpha2.APIExport) error {
	existing := &apisv1alpha2.APIExport{}
	err := p.kcpClient.Get(ctx, client.ObjectKeyFromObject(export), existing)
	if apierrors.IsNotFound(err) {
		return p.kcpClient.Create(ctx, export)
	}
	if err != nil {
		return fmt.Errorf("getting APIExport %s: %w", export.Name, err)
	}
	existing.Spec.Resources = export.Spec.Resources
	existing.Spec.PermissionClaims = export.Spec.PermissionClaims
	return p.kcpClient.Update(ctx, existing)
}
