package kafkachannel

import (
	"context"
	"embed"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/manifest"
)

//go:embed *.yaml
var yaml embed.FS

func GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "messaging.knative.dev", Version: "v1beta1", Resource: "kafkachannels"}
}

const defaultKafkaPartitions = 10
const defaultKafkaReplicationFactor = 3
const defaultKafkaRetentionDuration = "PT168H"

// Install will create an KafkaChannel resource, using the latest version, augmented with the config fn options.
func Install(name string, opts ...manifest.CfgFn) feature.StepFn {
	cfg := map[string]interface{}{
		"name":              name,
		"version":           GVR().Version,
		"numPartitions":     defaultKafkaPartitions,
		"replicationFactor": defaultKafkaReplicationFactor,
		"retentionDuration": defaultKafkaRetentionDuration,
	}
	for _, fn := range opts {
		fn(cfg)
	}
	return func(ctx context.Context, t feature.T) {
		if _, err := manifest.InstallYamlFS(ctx, yaml, cfg); err != nil {
			t.Fatal(err)
		}
	}
}

// IsReady tests to see if a KafkaChannel becomes ready within the time given.
func IsReady(name string, timings ...time.Duration) feature.StepFn {
	return k8s.IsReady(GVR(), name, timings...)
}

// WithVersion overrides the default API version
func WithVersion(version string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		if version != "" {
			cfg["version"] = version
		}
	}
}

// WithNumPartitions adds the numPartitions config to a KafkaChannel spec.
func WithNumPartitions(numPartitions string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["numPartitions"] = numPartitions
	}
}

// WithReplicationFactor adds the replicationFactor config to a KafkaChannel spec.
func WithReplicationFactor(replicationFactor string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["replicationFactor"] = replicationFactor
	}
}

// WithRetentionDuration adds the retentionDuration config to a KafkaChannel spec.
func WithRetentionDuration(retentionDuration string) manifest.CfgFn {
	return func(cfg map[string]interface{}) {
		cfg["retentionDuration"] = retentionDuration
	}
}
