package soake2erekt

import (
	"context"
	"fmt"
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
	"strconv"
	"testing"
)

func eventshubReceiverFeature() *feature.Feature {
	f := feature.NewFeatureNamed("receiver-create")
	f.Setup("install eventshub receiver", eventshub.Install(receiverName, eventshub.StartReceiver))
	return f
}

func kafkaSourceScenarioTopicAndSinkSetupFeature(name string) *feature.Feature {
	f := feature.NewFeatureNamed("kafka-topic-and-sink-setup")
	f.Setup("install kafka topic", kafkatopic.Install(name))
	f.Setup("topic is ready", kafkatopic.IsReady(name))

	f.Setup("install kafkasink", kafkasink.Install(name, name,
		testpkg.BootstrapServersPlaintextArr))
	f.Setup("kafkasink is ready", kafkasink.IsReady(name))

	return f
}

func kafkaSourceScenarioInstallKafkaSourceFeature(topicName, kafkaSourceName string) *feature.Feature {
	f := feature.NewFeatureNamed("kafka-source-add")

	kafkaSourceOpts := []manifest.CfgFn{
		kafkasource.WithSink(service.AsKReference(receiverName), ""),
		kafkasource.WithTopics([]string{topicName}),
		kafkasource.WithBootstrapServers(testpkg.BootstrapServersPlaintextArr),
		kafkasource.WithConsumers(7),
	}

	f.Setup("install kafka source", kafkasource.Install(kafkaSourceName, kafkaSourceOpts...))
	return f
}

func kafkaSourceScenarioIsReadyKafkaSourceFeature(kafkaSourceName string) *feature.Feature {
	f := feature.NewFeatureNamed("kafka-source-isready")
	f.Setup("kafka source is ready", kafkasource.IsReady(kafkaSourceName))
	return f
}

func matchEvent(sink string, matcher cetest.EventMatcher, exact int) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		assert.OnStore(sink).MatchEvent(matcher).Exact(exact)(ctx, t)
	}
}

func kafkaSinkSendFeature(kafkaSinkName string, index int) *feature.Feature {
	f := feature.NewFeatureNamed("kafka-sink-send")

	e := cetest.FullEvent()
	e.SetData("text/json", "hello"+strconv.Itoa(index))

	f.Requirement("install eventshub sender", eventshub.Install("send-"+kafkaSinkName+"-"+strconv.Itoa(index),
		eventshub.StartSenderToResource(kafkasink.GVR(), kafkaSinkName),
		eventshub.InputEvent(e)),
	)

	return f
}

func verifyEventReceivedFeature(index int, eventsToBeReceived int) *feature.Feature {
	f := feature.NewFeatureNamed("verify-events-received")

	e := cetest.FullEvent()
	e.SetData("text/json", "hello"+strconv.Itoa(index))

	matcher := cetest.HasData(e.Data())
	f.Assert("eventshub receiver gets events", matchEvent(receiverName, matcher, eventsToBeReceived))

	return f
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

	soakTest := SoakTest{
		NamespacePrefix: "test-kafka-source-stable-",
		SetupFns: []func(copyId int) *feature.Feature{
			func(copyId int) *feature.Feature {
				return eventshubReceiverFeature()
			},
			func(copyId int) *feature.Feature {
				topicName := fmt.Sprintf("%s-%d", topicPrefix, copyId)
				return kafkaSourceScenarioTopicAndSinkSetupFeature(topicName)
			},
			func(copyId int) *feature.Feature {
				topicName := fmt.Sprintf("%s-%d", topicPrefix, copyId)
				return kafkaSourceScenarioInstallKafkaSourceFeature(topicName, kafkaSourceName)
			},
			func(copyId int) *feature.Feature {
				return kafkaSourceScenarioIsReadyKafkaSourceFeature(kafkaSourceName)
			},
		},
		IterationFns: []func(copyId int, iteration int) *feature.Feature{

			func(copyId, iteration int) *feature.Feature {
				kafkaSinkName := fmt.Sprintf("%s-%d", kafkaSinkPrefix, copyId)
				return kafkaSinkSendFeature(kafkaSinkName, iteration)
			},
			func(copyId, iteration int) *feature.Feature {
				return verifyEventReceivedFeature(iteration, 1)
			},
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

	soakTest := SoakTest{
		NamespacePrefix: "test-kafka-source-recreate-",
		SetupFns: []func(copyId int) *feature.Feature{
			func(copyId int) *feature.Feature {
				return eventshubReceiverFeature()
			},
			func(copyId int) *feature.Feature {
				topicName := fmt.Sprintf("%s-%d", topicPrefix, copyId)
				return kafkaSourceScenarioTopicAndSinkSetupFeature(topicName)
			},
		},
		IterationFns: []func(copyId int, iteration int) *feature.Feature{
			func(copyId, iteration int) *feature.Feature {
				topicName := fmt.Sprintf("%s-%d", topicPrefix, copyId)
				kafkaSourceName := fmt.Sprintf("%s-%d", kafkaSourcePrefix, iteration)
				return kafkaSourceScenarioInstallKafkaSourceFeature(topicName, kafkaSourceName)
			},
			func(copyId, iteration int) *feature.Feature {
				kafkaSourceName := fmt.Sprintf("%s-%d", kafkaSourcePrefix, iteration)
				return kafkaSourceScenarioIsReadyKafkaSourceFeature(kafkaSourceName)
			},
			func(copyId, iteration int) *feature.Feature {
				kafkaSinkName := fmt.Sprintf("%s-%d", kafkaSinkPrefix, copyId)
				return kafkaSinkSendFeature(kafkaSinkName, iteration)
			},
			func(copyId, iteration int) *feature.Feature {
				return verifyEventReceivedFeature(iteration, 1)
			},
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

	const max = 16

	// As part of this soak test, we crate 16 kafkasources, then wait for them to be ready, \
	// and finally send an event and verify an event was received 16 times
	iterationFuncs := make([]func(_copy, iteration int) *feature.Feature, max*2+2)
	for j := 0; j < max; j++ {
		j := j
		iterationFuncs[j] = func(copyId, iteration int) *feature.Feature {
			topicName := fmt.Sprintf("%s-%d", topicPrefix, copyId)
			kafkaSourceName := fmt.Sprintf("%s-%d-%d", kafkaSourcePrefix, iteration, j)
			return kafkaSourceScenarioInstallKafkaSourceFeature(topicName, kafkaSourceName)
		}
		iterationFuncs[max+j] = func(copyId, iteration int) *feature.Feature {
			kafkaSourceName := fmt.Sprintf("%s-%d-%d", kafkaSourcePrefix, iteration, j)
			return kafkaSourceScenarioIsReadyKafkaSourceFeature(kafkaSourceName)
		}
	}
	iterationFuncs[2*max] = func(copyId, iteration int) *feature.Feature {
		kafkaSinkName := fmt.Sprintf("%s-%d", kafkaSinkPrefix, copyId)
		return kafkaSinkSendFeature(kafkaSinkName, iteration)
	}
	iterationFuncs[2*max+1] = func(copyId, iteration int) *feature.Feature {
		return verifyEventReceivedFeature(iteration, max)
	}

	soakTest := SoakTest{
		NamespacePrefix: "test-kafka-source-addrm-",
		SetupFns: []func(copyId int) *feature.Feature{
			func(copyId int) *feature.Feature {
				return eventshubReceiverFeature()
			},
			func(copyId int) *feature.Feature {
				topicName := fmt.Sprintf("%s-%d", topicPrefix, copyId)
				return kafkaSourceScenarioTopicAndSinkSetupFeature(topicName)
			},
		},
		IterationFns: iterationFuncs,
	}

	RunSoakTestWithDefaultCopies(t, soakTest)
}
