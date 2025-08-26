package kafka_test

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func runTest(
	t *testing.T,
	name string,
	fn func(t *testing.T, testCtx tests.TestContext),
	features tests.Features,
	parallel bool,
) {
	t.Run(name, func(t *testing.T) {
		if parallel {
			t.Parallel()
		}
		testID := tests.NewTestID()

		t.Run(string(testID), func(t *testing.T) {
			tCtx := tests.TestContext{
				TestID:   testID,
				Features: features,
			}

			fn(t, tCtx)
		})
	})
}

func testBulkMessageHandlerPubSub(
	t *testing.T,
	features tests.Features,
	pubSubConstructor tests.PubSubConstructor,
	consumerGroupPubSubConstructor tests.ConsumerGroupPubSubConstructor,
) {
	testFuncs := []struct {
		Func        func(t *testing.T, tCtx tests.TestContext, pubSubConstructor tests.PubSubConstructor)
		NotParallel bool
	}{
		{Func: tests.TestPublishSubscribe},
		{Func: tests.TestConcurrentSubscribe},
		{Func: tests.TestConcurrentSubscribeMultipleTopics},
		{Func: tests.TestResendOnError},
		// TestNoAck is not executed on bulk consumer because the whole purpose
		// of that configuration is allowing the reception of multiple messages
		// without having to ACK the previous one. What matters in this model is
		// the order
		// {Func: tests.TestNoAck},
		{Func: tests.TestContinueAfterSubscribeClose},
		{Func: tests.TestConcurrentClose},
		{Func: tests.TestContinueAfterErrors},
		{Func: tests.TestPublishSubscribeInOrder},
		{Func: tests.TestPublisherClose},
		{Func: tests.TestTopic},
		{Func: tests.TestMessageCtx},
		{Func: tests.TestSubscribeCtx},
		{Func: tests.TestNewSubscriberReceivesOldMessages},
		{
			Func:        tests.TestReconnect,
			NotParallel: true,
		},
	}

	for i := range testFuncs {
		testFunc := testFuncs[i]

		runTest(
			t,
			getTestName(testFunc.Func),
			func(t *testing.T, testCtx tests.TestContext) {
				testFunc.Func(t, testCtx, pubSubConstructor)
			},
			features,
			!testFunc.NotParallel,
		)
	}

	runTest(
		t,
		getTestName(tests.TestConsumerGroups),
		func(t *testing.T, testCtx tests.TestContext) {
			tests.TestConsumerGroups(
				t,
				testCtx,
				consumerGroupPubSubConstructor,
			)
		},
		features,
		true,
	)
}

const defaultStressTestTestsCount = 10

func testBulkMessageHandlerPubSubStressTest(
	t *testing.T,
	features tests.Features,
	pubSubConstructor tests.PubSubConstructor,
	consumerGroupPubSubConstructor tests.ConsumerGroupPubSubConstructor,
) {
	stressTestsCount, _ := strconv.ParseInt(os.Getenv("STRESS_TEST_COUNT"), 10, 64)
	if stressTestsCount == 0 {
		stressTestsCount = defaultStressTestTestsCount
	}

	for i := 0; i < int(stressTestsCount); i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Parallel()
			testBulkMessageHandlerPubSub(t, features, pubSubConstructor, consumerGroupPubSubConstructor)
		})
	}
}

func getTestName(testFunc interface{}) string {
	fullName := runtime.FuncForPC(reflect.ValueOf(testFunc).Pointer()).Name()
	nameSliced := strings.Split(fullName, ".")

	return nameSliced[len(nameSliced)-1]
}
