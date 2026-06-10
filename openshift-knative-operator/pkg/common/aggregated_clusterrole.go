package common

import (
	mf "github.com/manifestival/manifestival"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// RemoveRulesFromAggregatedClusterRoles removes the "rules" field from
// ClusterRoles that have an aggregationRule set. The Kubernetes controller
// manager populates the rules for aggregated ClusterRoles automatically
// based on the aggregation selectors. When manifestival applies an aggregated
// ClusterRole with "rules: []" from the manifest, it temporarily resets
// the aggregated rules until the controller manager repopulates them,
// creating a brief RBAC blackout window on every reconciliation.
func RemoveRulesFromAggregatedClusterRoles() mf.Transformer {
	return func(u *unstructured.Unstructured) error {
		if u.GetKind() != "ClusterRole" {
			return nil
		}
		aggRule, found, err := unstructured.NestedMap(u.Object, "aggregationRule")
		if err != nil || !found || len(aggRule) == 0 {
			return nil
		}
		delete(u.Object, "rules")
		return nil
	}
}
