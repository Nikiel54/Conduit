// This file implements Tests 1-4 from the failure-mode test suite referenced
// throughout the build spec. Each test is a self-contained scenario that
// exercises one piece of queue semantics end-to-end over real HTTP.
//
// Tests 5-8 (concurrent consumers, crash recovery, fan-out, backpressure)
// depend on functionality from later steps (the dispatcher pattern, the WAL,
// topics, and depth limits respectively) and are not implemented here.
package tests

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestFailure1_BasicFIFOOrder publishes 1000 messages at the same priority
// and verifies they're delivered in the exact order they were published.
//
// This is the simplest possible correctness guarantee a queue can offer:
// within a single priority tier, it's a FIFO queue, not a bag. If this test
// fails, the bug is almost certainly in Queue.Enqueue/Dequeue's slice
// manipulation (e.g. popping from the wrong end).
func TestFailure1_BasicFIFOOrder(t *testing.T) {
	ts := newTestServer(t)

	const n = 1000
	want := make([]string, n)
	for i := 0; i < n; i++ {
		payload := payloadForIndex(i)
		want[i] = payload
		publishMessage(t, ts, "fifo-queue", payload, "medium", 3)
	}

	got := consumeMessages(t, ts, "fifo-queue", 0, n)
	if len(got) != n {
		t.Fatalf("expected %d messages, got %d", n, len(got))
	}

	for i, msg := range got {
		if msg.Payload != want[i] {
			t.Fatalf("message %d: expected payload %q, got %q", i, want[i], msg.Payload)
		}
	}
}

// payloadForIndex builds a distinguishable payload string for FIFO ordering
// checks ("msg-0", "msg-1", ...). Named as its own function so the loop body
// in TestFailure1_BasicFIFOOrder reads as "build the expected payload",
// without an inline fmt.Sprintf call competing for attention with the
// publish/assert logic around it.
func payloadForIndex(i int) string {
	return fmt.Sprintf("msg-%d", i)
}

// TestFailure2_PriorityOrderingStrict publishes 100 messages at each of the
// three priority levels (interleaved in publish order) and verifies that
// ALL high-priority messages are delivered before ANY medium, and ALL
// medium before ANY low.
//
// This is Architectural Decision 2 (strict preemption) under test: there is
// no tolerance, no "occasionally a low slips through." If even one of the
// first 100 messages isn't "high", priorityOrder in internal/broker/queue.go
// is wrong or Dequeue isn't checking buckets in the right order.
func TestFailure2_PriorityOrderingStrict(t *testing.T) {
	ts := newTestServer(t)

	const perPriority = 100

	// Publish interleaved: low, medium, high, low, medium, high, ...
	// Interleaving (rather than publishing all of one priority, then the
	// next) proves that dequeue order depends on PRIORITY, not on publish
	// order or arrival time.
	for i := 0; i < perPriority; i++ {
		publishMessage(t, ts, "priority-queue", "low-msg", "low", 3)
		publishMessage(t, ts, "priority-queue", "medium-msg", "medium", 3)
		publishMessage(t, ts, "priority-queue", "high-msg", "high", 3)
	}

	got := consumeMessages(t, ts, "priority-queue", 0, perPriority*3)
	if len(got) != perPriority*3 {
		t.Fatalf("expected %d messages, got %d", perPriority*3, len(got))
	}

	// First 100: all "high".
	for i := 0; i < perPriority; i++ {
		if got[i].Priority != "high" {
			t.Fatalf("message %d: expected priority \"high\", got %q (payload %q)", i, got[i].Priority, got[i].Payload)
		}
	}

	// Next 100: all "medium".
	for i := perPriority; i < 2*perPriority; i++ {
		if got[i].Priority != "medium" {
			t.Fatalf("message %d: expected priority \"medium\", got %q (payload %q)", i, got[i].Priority, got[i].Payload)
		}
	}

	// Last 100: all "low".
	for i := 2 * perPriority; i < 3*perPriority; i++ {
		if got[i].Priority != "low" {
			t.Fatalf("message %d: expected priority \"low\", got %q (payload %q)", i, got[i].Priority, got[i].Payload)
		}
	}
}

// TestFailure3_AckTimeoutRedelivery verifies that a message which is
// consumed but never acked becomes available again after its visibility
// timeout expires, and that its delivery_count reflects the redelivery.
//
// This is the core promise of "at-least-once delivery": a consumer that
// crashes (or just never acks) doesn't cause the message to be lost.
func TestFailure3_AckTimeoutRedelivery(t *testing.T) {
	ts := newTestServer(t)

	publishMessage(t, ts, "redelivery-queue", "important-task", "medium", 3)

	// First consume: use a short visibility timeout so we don't have to
	// wait the default 30 seconds for it to expire.
	first := consumeMessages(t, ts, "redelivery-queue", 1, 1)
	if len(first) != 1 {
		t.Fatalf("expected 1 message on first consume, got %d", len(first))
	}
	if first[0].DeliveryCount != 1 {
		t.Fatalf("expected delivery_count 1 on first delivery, got %d", first[0].DeliveryCount)
	}
	originalID := first[0].MessageID

	// Deliberately do not ack. Wait past the 1-second visibility timeout
	// (plus a buffer for scheduling jitter) so the broker's internal timer
	// has fired and redelivered the message.
	time.Sleep(shortVisibilityTimeout + 200*time.Millisecond)

	second := consumeMessages(t, ts, "redelivery-queue", 1, 1)
	if len(second) != 1 {
		t.Fatalf("expected 1 message on second consume (redelivery), got %d", len(second))
	}
	if second[0].MessageID != originalID {
		t.Fatalf("expected redelivered message_id %q, got %q", originalID, second[0].MessageID)
	}
	if second[0].DeliveryCount != 2 {
		t.Fatalf("expected delivery_count 2 after redelivery, got %d", second[0].DeliveryCount)
	}
}

// TestFailure4_DeadLetterRouting verifies that a message exceeding
// max_retries is moved to the dead-letter queue instead of being
// redelivered indefinitely, and that it becomes visible via
// GET /queues/{name}/dlq.
//
// Worked trace for max_retries=3 (see the timeoutCh case in
// internal/broker/queue.go's dispatcher loop for the full logic):
//
//	consume 1 -> delivery_count 1, timeout -> requeued (1 < 3)
//	consume 2 -> delivery_count 2, timeout -> requeued (2 < 3)
//	consume 3 -> delivery_count 3, timeout -> DLQ'd    (3 >= 3)
//	consume 4 -> main queue empty; message now sits in the DLQ
func TestFailure4_DeadLetterRouting(t *testing.T) {
	ts := newTestServer(t)

	const maxRetries = 3
	originalID := publishMessage(t, ts, "dlq-queue", "poison-pill", "medium", maxRetries)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		got := consumeMessages(t, ts, "dlq-queue", 1, 1)
		if len(got) != 1 {
			t.Fatalf("attempt %d: expected 1 message, got %d", attempt, len(got))
		}
		if got[0].MessageID != originalID {
			t.Fatalf("attempt %d: expected message_id %q, got %q", attempt, originalID, got[0].MessageID)
		}
		if got[0].DeliveryCount != attempt {
			t.Fatalf("attempt %d: expected delivery_count %d, got %d", attempt, attempt, got[0].DeliveryCount)
		}

		// Let the visibility timeout expire so the broker decides whether
		// to redeliver (attempts 1-2) or move to the DLQ (attempt 3).
		time.Sleep(shortVisibilityTimeout + 200*time.Millisecond)
	}

	// 4th attempt: the message has been moved to the DLQ after attempt 3's
	// timeout. The main queue should now be empty.
	fourth := consumeMessages(t, ts, "dlq-queue", 1, 1)
	if len(fourth) != 0 {
		t.Fatalf("expected 0 messages on 4th consume (message should be in DLQ), got %d", len(fourth))
	}

	dlq := getDLQ(t, ts, "dlq-queue")
	if len(dlq) != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", len(dlq))
	}
	if dlq[0].MessageID != originalID {
		t.Fatalf("expected DLQ message_id %q, got %q", originalID, dlq[0].MessageID)
	}
	if dlq[0].DeliveryCount != maxRetries {
		t.Fatalf("expected DLQ delivery_count %d, got %d", maxRetries, dlq[0].DeliveryCount)
	}
}

// TestFailure6_ConcurrentConsumerCorrectness publishes 1000 messages and then
// runs 4 consumers concurrently, each looping "consume 1 -> ack -> repeat"
// until it sees an empty response. It asserts that every message was
// delivered to exactly one consumer: the total number of messages received
// across all consumers is 1000, and no message_id appears in more than one
// consumer's list.
//
// This is Subsystem 5 (the dispatcher goroutine pattern) under test: a
// single goroutine (Queue.run, added in Step 3) serializes every Dequeue
// request. Even though 4 goroutines are racing to call
// POST /queues/{name}/consume, each of the 1000 messages can only be popped
// from pending once -- there's no shared state for two dispatcher requests
// to race over. Run with `go test -race`; that's the whole point of this
// test (and of Step 3).
//
// Each consumer's results are written only to its own slot in received, so
// there's no shared mutable state between the goroutines themselves either
// -- the assertions below run after wg.Wait(), which happens-after every
// goroutine's write to its slot.
func TestFailure6_ConcurrentConsumerCorrectness(t *testing.T) {
	ts := newTestServer(t)

	const (
		numMessages  = 1000
		numConsumers = 4
	)

	for i := 0; i < numMessages; i++ {
		publishMessage(t, ts, "concurrent-queue", payloadForIndex(i), "medium", 3)
	}

	received := make([][]string, numConsumers)

	var wg sync.WaitGroup
	for c := 0; c < numConsumers; c++ {
		wg.Add(1)
		go func(consumerIdx int) {
			defer wg.Done()

			var ids []string
			for {
				got := consumeMessages(t, ts, "concurrent-queue", 0, 1)
				if len(got) == 0 {
					// Pending is empty: every message has been dequeued by
					// some consumer (possibly this one, possibly another).
					break
				}

				for _, msg := range got {
					ids = append(ids, msg.MessageID)
					if status := ackMessage(t, ts, msg.MessageID); status != http.StatusNoContent {
						t.Errorf("consumer %d: ack %s: expected 204, got %d", consumerIdx, msg.MessageID, status)
					}
				}
			}

			received[consumerIdx] = ids
		}(c)
	}
	wg.Wait()

	seen := make(map[string]int) // message_id -> number of consumers that received it
	total := 0
	for _, ids := range received {
		total += len(ids)
		for _, id := range ids {
			seen[id]++
		}
	}

	if total != numMessages {
		t.Fatalf("expected %d total messages received across all consumers, got %d", numMessages, total)
	}
	if len(seen) != numMessages {
		t.Fatalf("expected %d distinct message_ids, got %d", numMessages, len(seen))
	}
	for id, count := range seen {
		if count > 1 {
			t.Errorf("message_id %q delivered to %d consumers, want 1", id, count)
		}
	}
}
