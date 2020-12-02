package test

import (
	"context"
	"fmt"
	networking "k8s.io/api/networking/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"
)

func ServiceMeshControlPlaneV1(name, namespace string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "maistra.io/v1",
			"kind":       "ServiceMeshControlPlane",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
		},
	}
}

func AddServiceMeshControlPlaneV1IngressGatewaySecretVolume(smcp *unstructured.Unstructured, name, secretName, mountPath string) error {
	secretVolume := make(map[string]interface{})
	secretVolume["name"] = name
	secretVolume["secretName"] = secretName
	secretVolume["mountPath"] = mountPath

	secretVolumes, found, _ := unstructured.NestedSlice(smcp.Object, "spec", "istio", "gateways", "istio-ingressgateway", "secretVolumes")
	if found {
		secretVolumes = append(secretVolumes, secretVolume)
	} else {
		secretVolumes = make([]interface{}, 1)
		secretVolumes[0] = secretVolume
	}

	return unstructured.SetNestedSlice(smcp.Object, secretVolumes, "spec", "istio", "gateways", "istio-ingressgateway", "secretVolumes")
}

func ServiceMeshMemberRollV1(name, namespace string, members ...string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "maistra.io/v1",
			"kind":       "ServiceMeshMemberRoll",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"members": members,
			},
		},
	}
}

// IstioGateway creates an Istio Gateway for HTTP traffic via istio-ingressgateway
func IstioGateway(name, namespace string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "networking.istio.io/v1alpha3",
			"kind":       "Gateway",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"selector": map[string]interface{}{
					"istio": "ingressgateway",
				},
				"servers": []map[string]interface{}{
					{
						"port": map[string]interface{}{
							"number":   80,
							"name":     "http",
							"protocol": "HTTP",
						},
						"hosts": []string{
							"*",
						},
					},
				},
			},
		},
	}
}

// IstioGatewayWithTLS creates an Istio Gateway for HTTPS traffic via istio-ingressgateway
// for a specific host with a custom domain and certificates.
// The certificate/privateKey must be already mounted on the istio-ingressgateway on the given paths
func IstioGatewayWithTLS(name, namespace string, host, privateKeyPath, serverCertificatePath string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "networking.istio.io/v1alpha3",
			"kind":       "Gateway",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"selector": map[string]interface{}{
					"istio": "ingressgateway",
				},
				"servers": []map[string]interface{}{
					{
						"port": map[string]interface{}{
							"number":   80,
							"name":     "http",
							"protocol": "HTTP",
						},
						"hosts": []string{
							"*",
						},
					},
					{
						"port": map[string]interface{}{
							"number":   443,
							"name":     "https",
							"protocol": "HTTPS",
						},
						"tls": map[string]interface{}{
							"mode":              "SIMPLE",
							"privateKey":        privateKeyPath,
							"serverCertificate": serverCertificatePath,
						},
						"hosts": []string{
							host,
						},
					},
				},
			},
		},
	}
}

// IstioVirtualServiceForKnativeServiceWithCustomDomain creates an Istio VirtualService to
// rewrite the host header of a custom domain with the ksvc's svc hostname
func IstioVirtualServiceForKnativeServiceWithCustomDomain(service *servingv1.Service, gatewayName string, customHostname string) *unstructured.Unstructured {
	serviceHostname := fmt.Sprintf("%s.%s.svc", service.Name, service.Namespace)
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "networking.istio.io/v1alpha3",
			"kind":       "VirtualService",
			"metadata": map[string]interface{}{
				"name":      service.Name,
				"namespace": service.Namespace,
			},
			"spec": map[string]interface{}{
				"hosts": []string{
					customHostname,
				},
				"gateways": []string{
					gatewayName,
				},
				"http": []map[string]interface{}{
					{
						"rewrite": map[string]interface{}{
							"authority": serviceHostname,
						},
						"route": []map[string]interface{}{
							{
								"destination": map[string]interface{}{
									"host": serviceHostname,
									"port": map[string]interface{}{
										"number": 80,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// IstioServiceEntryForKnativeServiceTowardsKourier creates an Istio ServiceEntry
// for ksvc's svc hostname routing towards the knative kourier-internal gateway
func IstioServiceEntryForKnativeServiceTowardsKourier(service *servingv1.Service) *unstructured.Unstructured {
	serviceHostname := fmt.Sprintf("%s.%s.svc", service.Name, service.Namespace)
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "networking.istio.io/v1alpha3",
			"kind":       "ServiceEntry",
			"metadata": map[string]interface{}{
				"namespace": service.Namespace,
				"name":      serviceHostname,
			},
			"spec": map[string]interface{}{
				"hosts": []string{
					serviceHostname,
				},
				"location": "MESH_EXTERNAL",
				"endpoints": []map[string]interface{}{
					{
						"address": "kourier-internal.knative-serving-ingress.svc",
					},
				},
				"ports": []map[string]interface{}{
					{
						"number":   80,
						"name":     "http",
						"protocol": "HTTP",
					},
				},
				"resolution": "DNS",
			},
		},
	}
}

func serviceMeshControlPlaneV1Schema() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "maistra.io",
		Version:  "v1",
		Resource: "servicemeshcontrolplanes",
	}
}

func serviceMeshControlPlaneV2Schema() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "maistra.io",
		Version:  "v2",
		Resource: "servicemeshcontrolplanes",
	}
}

func CreateServiceMeshControlPlaneV1(ctx *Context, smcp *unstructured.Unstructured) *unstructured.Unstructured {
	// When cleaning-up SMCP, wait until it doesn't exist, as it takes a while, which would break subsequent tests
	ctx.AddToCleanup(func() error {
		ctx.T.Logf("Waiting for ServiceMeshControlPlane %q to not exist", smcp.GetName())
		_, err := WaitForUnstructuredState(ctx, serviceMeshControlPlaneV1Schema(), smcp.GetName(), smcp.GetNamespace(), DoesUnstructuredNotExist)
		return err
	})
	return CreateUnstructured(ctx, serviceMeshControlPlaneV1Schema(), smcp)
}

func WaitForServiceMeshControlPlaneV2Ready(ctx *Context, name, namespace string) {
	_, err := WaitForUnstructuredState(ctx, serviceMeshControlPlaneV2Schema(), name, namespace, IsUnstructuredReady)
	if err != nil {
		ctx.T.Fatalf("Error waiting for ServiceMeshControlPlane readiness: %v", err)
	}
}

func CreateServiceMeshMemberRollV1(ctx *Context, smmr *unstructured.Unstructured) *unstructured.Unstructured {
	smmrGvr := schema.GroupVersionResource{
		Group:    "maistra.io",
		Version:  "v1",
		Resource: "servicemeshmemberrolls",
	}

	return CreateUnstructured(ctx, smmrGvr, smmr)
}

func CreateIstioGateway(ctx *Context, gateway *unstructured.Unstructured) *unstructured.Unstructured {
	gatewayGvr := schema.GroupVersionResource{
		Group:    "networking.istio.io",
		Version:  "v1alpha3",
		Resource: "gateways",
	}

	return CreateUnstructured(ctx, gatewayGvr, gateway)
}

func CreateIstioServiceEntry(ctx *Context, serviceEntry *unstructured.Unstructured) *unstructured.Unstructured {
	serviceEntryGvr := schema.GroupVersionResource{
		Group:    "networking.istio.io",
		Version:  "v1alpha3",
		Resource: "serviceentries",
	}

	return CreateUnstructured(ctx, serviceEntryGvr, serviceEntry)
}

func CreateIstioVirtualService(ctx *Context, virtualService *unstructured.Unstructured) *unstructured.Unstructured {
	virtualServiceGvr := schema.GroupVersionResource{
		Group:    "networking.istio.io",
		Version:  "v1alpha3",
		Resource: "virtualservices",
	}

	return CreateUnstructured(ctx, virtualServiceGvr, virtualService)
}

func AllowFromServingSystemNamespaceNetworkPolicy(namespace string) *networking.NetworkPolicy {
	return &networking.NetworkPolicy{
		ObjectMeta: meta.ObjectMeta{
			Name:      "allow-from-serving-system-namespace",
			Namespace: namespace,
		},
		Spec: networking.NetworkPolicySpec {
			Ingress: []networking.NetworkPolicyIngressRule {
				{
					From: []networking.NetworkPolicyPeer {
						{
							NamespaceSelector: &meta.LabelSelector{
								MatchLabels: map[string]string{
									"knative.openshift.io/system-namespace": "true",
								},
							},
						},
					},
				},
			},
			PolicyTypes: []networking.PolicyType{
				networking.PolicyTypeIngress,
			},
		},
	}
}

func CreateNetworkPolicy(ctx *Context, networkPolicy *networking.NetworkPolicy) *networking.NetworkPolicy {
	networkPolicy, err := ctx.Clients.Kube.NetworkingV1().NetworkPolicies(networkPolicy.Namespace).Create(context.Background(), networkPolicy, meta.CreateOptions{})
	if err != nil {
		ctx.T.Fatalf("Error creating NetworkPolicy %s: %v", networkPolicy.GetName(), err)
	}

	ctx.AddToCleanup(func() error {
		ctx.T.Logf("Cleaning up NetworkPolicy %s", networkPolicy.GetName())
		return ctx.Clients.Kube.NetworkingV1().NetworkPolicies(networkPolicy.Namespace).Delete(context.Background(), networkPolicy.GetName(), meta.DeleteOptions{})
	})

	return networkPolicy
}

func LabelNamespace(ctx *Context, namespace, key, value string) {
	_, err := ctx.Clients.Kube.CoreV1().Namespaces().Patch(
		context.Background(),
		namespace,
		types.MergePatchType,
		[]byte(fmt.Sprintf("{\"metadata\":{\"labels\":{\"%s\":\"%s\"}}}", key, value)),
		meta.PatchOptions{})
	if err != nil {
		ctx.T.Fatalf("Error labelling namespace %q: %v", namespace, err)
	}
}
