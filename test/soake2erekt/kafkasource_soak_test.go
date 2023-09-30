package soake2erekt

import (
	"context"
	"fmt"
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasink"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"
	"knative.dev/reconciler-test/pkg/resources/service"
)

const (
	receiverNameKey = "receiver"
	kafkaTopicKey   = "kafkaTopic"
	kafkaSinkKey    = "kafkaSink"
	kafkaSourceKey  = "kafkaSource"

	eventSenderKey = "eventSender"
)

func eventshubReceiverFeature(names map[string]string) *feature.Feature {
	f := feature.NewFeatureNamed("receiver-create")
	f.Setup("install eventshub receiver", eventshub.Install(names[receiverNameKey], eventshub.StartReceiver))
	return f
}

func kafkaSourceScenarioTopicAndSinkSetupFeature(names map[string]string) *feature.Feature {
	f := feature.NewFeatureNamed("kafka-topic-and-sink-setup")
	f.Setup("install kafka topic", kafkatopic.Install(names[kafkaTopicKey]))
	f.Setup("topic is ready", kafkatopic.IsReady(names[kafkaTopicKey]))

	f.Setup("install kafkasink", kafkasink.Install(names[kafkaSinkKey], names[kafkaTopicKey],
		testpkg.BootstrapServersPlaintextArr))
	f.Setup("kafkasink is ready", kafkasink.IsReady(names[kafkaSinkKey]))

	return f
}

func kafkaSourceScenarioInstallKafkaSourceFeature(names map[string]string) *feature.Feature {
	f := feature.NewFeatureNamed("kafka-source-add")

	kafkaSourceOpts := []manifest.CfgFn{
		kafkasource.WithSink(service.AsKReference(names[receiverNameKey]), ""),
		kafkasource.WithTopics([]string{names[kafkaTopicKey]}),
		kafkasource.WithBootstrapServers(testpkg.BootstrapServersPlaintextArr),
		kafkasource.WithConsumers(7),
	}

	f.Setup("install kafka source", kafkasource.Install(names[kafkaSourceKey], kafkaSourceOpts...))
	return f
}

func kafkaSourceScenarioIsReadyKafkaSourceFeature(names map[string]string) *feature.Feature {
	f := feature.NewFeatureNamed("kafka-source-isready")
	f.Setup("kafka source is ready", kafkasource.IsReady(names[kafkaSourceKey]))
	return f
}

func matchEvent(sink string, matcher cetest.EventMatcher, exact int) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		assert.OnStore(sink).MatchEvent(matcher).Exact(exact)(ctx, t)
	}
}

func kafkaSinkSendFeature(names map[string]string) *feature.Feature {
	f := feature.NewFeatureNamed("kafka-sink-send")

	e := cetest.FullEvent()
	e.SetData("text/json", names[eventSenderKey])

	f.Requirement("install eventshub sender", eventshub.Install(names[eventSenderKey],
		eventshub.StartSenderToResource(kafkasink.GVR(), names[kafkaSinkKey]),
		eventshub.InputEvent(e)),
	)

	return f
}

func verifyEventReceivedFeature(names map[string]string, eventsToBeReceived int) *feature.Feature {
	f := feature.NewFeatureNamed("verify-events-received")

	e := cetest.FullEvent()
	e.SetData("text/json", names[eventSenderKey])

	matcher := cetest.HasData(e.Data())
	f.Assert("eventshub receiver gets events", matchEvent(names[receiverNameKey], matcher, eventsToBeReceived))

	return f
}

func verifySingleEventReceivedFeature(names map[string]string) *feature.Feature {
	return verifyEventReceivedFeature(names, 1)
}

/*
TestKafkaSourceStableSoak on each iteration,
sends an event to a kafkasource
verifies an event is received
*/
func TestKafkaSourceStableSoak(t *testing.T) {
	t.Parallel()

	topicPrefix := feature.MakeRandomK8sName("topic")
	kafkaSinkPrefix := topicPrefix
	kafkaSourceName := feature.MakeRandomK8sName("kafkaSource")
	senderPrefix := feature.MakeRandomK8sName("sender")

	namesFn := func(fn func(map[string]string) *feature.Feature) func(copyId, iteration int) *feature.Feature {
		return func(copyId, iteration int) *feature.Feature {
			return fn(map[string]string{
				receiverNameKey: "receiver",
				kafkaTopicKey:   fmt.Sprintf("%s-%d", topicPrefix, copyId),
				kafkaSinkKey:    fmt.Sprintf("%s-%d", kafkaSinkPrefix, copyId),
				kafkaSourceKey:  kafkaSourceName,
				eventSenderKey:  fmt.Sprintf("%s-%d-%d", senderPrefix, copyId, iteration),
			})
		}
	}

	soakTest := SoakTest{
		NamespacePrefix: "test-kafka-source-stable-",
		SetupFns: []func(copyId, iteration int) *feature.Feature{
			namesFn(eventshubReceiverFeature),
			namesFn(kafkaSourceScenarioTopicAndSinkSetupFeature),
			namesFn(kafkaSourceScenarioInstallKafkaSourceFeature),
			namesFn(kafkaSourceScenarioIsReadyKafkaSourceFeature),
		},
		IterationFns: []func(copyId, iteration int) *feature.Feature{
			namesFn(kafkaSinkSendFeature),
			namesFn(verifySingleEventReceivedFeature),
		},
	}

	RunSoakTestWithDefaultCopies(t, soakTest)
}

/*
TestKafkaSourceRecreateSoak on each iteration,
creates a kafkasources from a single topic
sends an event
verifies an event is received
*/
func TestKafkaSourceRecreateSoak(t *testing.T) {
	t.Parallel()

	topicPrefix := feature.MakeRandomK8sName("topic")
	kafkaSinkPrefix := topicPrefix
	kafkaSourcePrefix := feature.MakeRandomK8sName("kafkaSource")
	senderPrefix := feature.MakeRandomK8sName("sender")

	namesFn := func(fn func(map[string]string) *feature.Feature) func(copyId, iteration int) *feature.Feature {
		return func(copyID, iteration int) *feature.Feature {
			return fn(map[string]string{
				receiverNameKey: "receiver",
				kafkaTopicKey:   fmt.Sprintf("%s-%d", topicPrefix, copyID),
				kafkaSinkKey:    fmt.Sprintf("%s-%d", kafkaSinkPrefix, copyID),
				kafkaSourceKey:  fmt.Sprintf("%s-%d", kafkaSourcePrefix, iteration),
				eventSenderKey:  fmt.Sprintf("%s-%d-%d", senderPrefix, copyID, iteration),
			})
		}
	}

	soakTest := SoakTest{
		NamespacePrefix: "test-kafka-source-recreate-",
		SetupFns: []func(copyID, iteration int) *feature.Feature{
			namesFn(eventshubReceiverFeature),
			namesFn(kafkaSourceScenarioTopicAndSinkSetupFeature),
		},
		IterationFns: []func(copyID, iteration int) *feature.Feature{
			namesFn(kafkaSourceScenarioInstallKafkaSourceFeature),
			namesFn(kafkaSourceScenarioIsReadyKafkaSourceFeature),
			namesFn(kafkaSinkSendFeature),
			namesFn(verifySingleEventReceivedFeature),
		},
	}

	RunSoakTestWithDefaultCopies(t, soakTest)
}

/*
TestKafkaSourceAddingAndRemovingSoak on each iteration,
creates 16 kafkasources from a single topic
sends an event
verifies an event is received 16 times
*/
func TestKafkaSourceAddingAndRemovingSoak(t *testing.T) {
	t.Parallel()

	topicPrefix := feature.MakeRandomK8sName("topic")
	kafkaSinkPrefix := topicPrefix
	kafkaSourcePrefix := feature.MakeRandomK8sName("kafkaSource")
	senderPrefix := feature.MakeRandomK8sName("sender")

	const max = 16

	names := func(copyID, iteration int) map[string]string {
		return map[string]string{
			receiverNameKey: "receiver",
			kafkaTopicKey:   fmt.Sprintf("%s-%d", topicPrefix, copyID),
			kafkaSinkKey:    fmt.Sprintf("%s-%d", kafkaSinkPrefix, copyID),
			// kafkaSourceKey:  not used, generated below
			eventSenderKey: fmt.Sprintf("%s-%d-%d", senderPrefix, copyID, iteration),
		}
	}

	namesFn := func(fn func(map[string]string) *feature.Feature) func(copyID, iteration int) *feature.Feature {
		return func(copyID, iteration int) *feature.Feature {
			return fn(names(copyID, iteration))
		}
	}

	// As part of this soak test, we crate 16 kafkasources, then wait for them to be ready,
	// and finally send an event and verify an event was received 16 times
	iterationFuncs := make([]func(copyID, iteration int) *feature.Feature, max*2+2)
	for j := 0; j < max; j++ {
		j := j

		iterationFuncs[j] = func(copyID, iteration int) *feature.Feature {
			ns := names(copyID, iteration)
			ns[kafkaSourceKey] = fmt.Sprintf("%s-%d-%d", kafkaSourcePrefix, iteration, j)
			return kafkaSourceScenarioInstallKafkaSourceFeature(ns)
		}
		iterationFuncs[max+j] = func(copyID, iteration int) *feature.Feature {
			ns := names(copyID, iteration)
			ns[kafkaSourceKey] = fmt.Sprintf("%s-%d-%d", kafkaSourcePrefix, iteration, j)
			return kafkaSourceScenarioIsReadyKafkaSourceFeature(ns)
		}
	}
	iterationFuncs[2*max] = namesFn(kafkaSinkSendFeature)
	iterationFuncs[2*max+1] = func(copyID, iteration int) *feature.Feature {
		return verifyEventReceivedFeature(names(copyID, iteration), max)
	}

	soakTest := SoakTest{
		NamespacePrefix: "test-kafka-source-addrm-",
		SetupFns: []func(copyID, iteration int) *feature.Feature{
			namesFn(eventshubReceiverFeature),
			namesFn(kafkaSourceScenarioTopicAndSinkSetupFeature),
		},
		IterationFns: iterationFuncs,
	}

	RunSoakTestWithDefaultCopies(t, soakTest)
}
