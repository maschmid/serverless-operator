package common

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestRemoveRulesFromAggregatedClusterRoles(t *testing.T) {
	transform := RemoveRulesFromAggregatedClusterRoles()

	tests := []struct {
		name          string
		obj           *unstructured.Unstructured
		expectRules   bool
	}{
		{
			name: "aggregated ClusterRole has rules removed",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "rbac.authorization.k8s.io/v1",
					"kind":       "ClusterRole",
					"metadata": map[string]interface{}{
						"name": "channelable-manipulator",
					},
					"aggregationRule": map[string]interface{}{
						"clusterRoleSelectors": []interface{}{
							map[string]interface{}{
								"matchLabels": map[string]interface{}{
									"duck.knative.dev/channelable": "true",
								},
							},
						},
					},
					"rules": []interface{}{},
				},
			},
			expectRules: false,
		},
		{
			name: "non-aggregated ClusterRole keeps rules",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "rbac.authorization.k8s.io/v1",
					"kind":       "ClusterRole",
					"metadata": map[string]interface{}{
						"name": "imc-channelable-manipulator",
					},
					"rules": []interface{}{
						map[string]interface{}{
							"apiGroups": []interface{}{"messaging.knative.dev"},
							"resources": []interface{}{"inmemorychannels", "inmemorychannels/status"},
							"verbs":     []interface{}{"create", "get", "list", "watch", "update", "patch"},
						},
					},
				},
			},
			expectRules: true,
		},
		{
			name: "non-ClusterRole is not affected",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "rbac.authorization.k8s.io/v1",
					"kind":       "Role",
					"metadata": map[string]interface{}{
						"name": "some-role",
					},
					"rules": []interface{}{
						map[string]interface{}{
							"apiGroups": []interface{}{""},
							"resources": []interface{}{"configmaps"},
							"verbs":     []interface{}{"get"},
						},
					},
				},
			},
			expectRules: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := transform(tt.obj)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			_, hasRules := tt.obj.Object["rules"]
			if hasRules != tt.expectRules {
				t.Errorf("expected rules present=%v, got %v", tt.expectRules, hasRules)
			}
		})
	}
}
