package broker

import (
	"context"
	"errors"
	"time"
)

// enum priority types
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityMedium Priority = "medium"
	PriorityLow    Priority = "low"
)


// priorityOrder defines dequeue precedence: high, med, low.
var priorityOrder = [...]Priority{PriorityHigh, PriorityMedium, PriorityLow}

// DefaultMaxRetries is used when a publish request omits max_retries
const DefaultMaxRetries = 3

// DefaultVisibilityTimeout is used when a consume request omits
// visibility_timeout_seconds.
const DefaultVisibilityTimeout = 30 * time.Second

// Message is a single unit of work flowing through a queue.
type Message struct {
	ID string

	// Payload is the producer-supplied message body.
	// I don't really care what is in here, it can be anything tbh.
	Payload string

	Priority   Priority
	MaxRetries int

	// DeliveryCount is incremented every time this message is handed to a
	// consumer via Dequeue.
	DeliveryCount int

	// QueuedAt is the time this message was published, set once and never changed.
	QueuedAt time.Time
}


// --- Dispatcher request/response types --------------------------------------
//
// Step 3: every operation that touches a queue's pending/inFlight/dlq state
// is performed by a single goroutine, run() (started by NewQueue). All other
// goroutines -- HTTP handlers calling Enqueue/Dequeue/Ack, and the visibility
// timeout callbacks in consumer.go -- talk to it by sending one of these
// request types on a channel and waiting for a response.

// enqueueRequest asks the dispatcher to append msg to its priority bucket.
// done is closed after appendage, so it blocks until successful enqueue.
type enqueueRequest struct {
	msg  *Message
	done chan struct{}
}

// dequeueRequest asks the dispatcher for the next message, if any.
// VisibilityTimeout is used here to check ack happened.
type dequeueRequest struct {
	visibilityTimeout time.Duration
	resp              chan dequeueResult
}

// dequeueResult is the dispatcher's reply to a dequeueRequest.
// If no msgs available, msg is nil and ok is false.
type dequeueResult struct {
	msg *Message
	ok  bool
}

// ackRequest asks the dispatcher to acknowledge messageID: stop its
// visibility timer and remove it from inFlight.
type ackRequest struct {
	messageID string
	resp      chan error
}


// Queue is a single named queue: three priority buckets of pending
// messages, an in-flight map for messages currently out for delivery
// (visibility timeout running), and a dead-letter queue for messages that
// exceeded max_retries.
//
// All of that state lives as local variables inside run() -- Queue itself
// holds no pending/inFlight/dlq fields and no mutex. Every method below
// sends a request on the matching channel and waits for run() to reply.
type Queue struct {
	name string

	enqueueCh chan enqueueRequest
	dequeueCh chan dequeueRequest
	ackCh     chan ackRequest
	dlqCh     chan dlqRequest
	timeoutCh chan timeoutEvent

	cancel context.CancelFunc
	done   chan struct{} // closed by run() when it returns
}


// NewQueue creates an empty queue with the given name and starts its
// dispatcher goroutine (run). 
// Queues are created lazily by the Broker on first reference, so only when needed.
func NewQueue(name string) *Queue {
	ctx, cancel := context.WithCancel(context.Background())

	q := &Queue{
		name: name,

		enqueueCh: make(chan enqueueRequest),
		dequeueCh: make(chan dequeueRequest),
		ackCh:     make(chan ackRequest),
		dlqCh:     make(chan dlqRequest),
		timeoutCh: make(chan timeoutEvent),

		cancel: cancel,
		done:   make(chan struct{}),
	}

	go q.run(ctx)

	return q
}


// run is the dispatcher goroutine: the single owner of pending, inFlight,
// and dlq for this queue's entire lifetime. It exits only when ctx is
// canceled.
func (q *Queue) run(ctx context.Context) {
	defer close(q.done)

	// priority buckets for pending messages.
	pending := map[Priority][]*Message{
		PriorityHigh:   {},
		PriorityMedium: {},
		PriorityLow:    {},
	}
	inFlight := make(map[string]*inFlightEntry) // inflight msgs
	var dlq []*Message // dead msgs

	for {
		select {
		case <-ctx.Done():
			// Stop every outstanding visibility timer. 
			for _, entry := range inFlight {
				entry.timer.Stop()
			}
			return

		case req := <-q.enqueueCh:
			pending[req.msg.Priority] = append(pending[req.msg.Priority], req.msg)
			close(req.done)

		case req := <-q.dequeueCh:
			var result dequeueResult

			for _, priority := range priorityOrder {
				bucket := pending[priority]
				if len(bucket) == 0 {
					continue
				}

				msg := bucket[0] // pop top of this bucket
				pending[priority] = bucket[1:]
				msg.DeliveryCount++

				inFlight[msg.ID] = q.startVisibilityTimer(ctx, msg, req.visibilityTimeout)

				snapshot := *msg
				result = dequeueResult{msg: &snapshot, ok: true}
				break
			}

			req.resp <- result

		case req := <-q.ackCh:
			entry, ok := inFlight[req.messageID]
			if !ok {
				req.resp <- ErrMessageNotFound
			} else {
				entry.timer.Stop()
				delete(inFlight, req.messageID)
				req.resp <- nil
			}

		case req := <-q.dlqCh:
			req.resp <- dlqSnapshot(dlq)

		case ev := <-q.timeoutCh:
			entry, ok := inFlight[ev.messageID]
			if ok {
				delete(inFlight, ev.messageID)

				msg := entry.message
				if msg.DeliveryCount >= msg.MaxRetries {
					dlq = append(dlq, msg)
				} else {
					// Redeliver to the back of this priority's bucket
					pending[msg.Priority] = append(pending[msg.Priority], msg)
				}
			}
			// else: already acked between the timer firing and run()
			// picking up this event -- nothing to do.
		}
	}
}

// Enqueue adds a new message to the queue's pending buffer for its
// priority; blocks until the dispatcher has appended the message.
func (q *Queue) Enqueue(msg *Message) {
	req := enqueueRequest{msg: msg, done: make(chan struct{})}
	q.enqueueCh <- req
	<-req.done
}

// Dequeue removes and returns the next message according to strict
// priority order.
// returns (nil, false) if every priority bucket is empty.
func (q *Queue) Dequeue(visibilityTimeout time.Duration) (*Message, bool) {
	req := dequeueRequest{visibilityTimeout: visibilityTimeout, resp: make(chan dequeueResult)}
	q.dequeueCh <- req
	res := <-req.resp
	return res.msg, res.ok
}

// ErrMessageNotFound is returned by Queue.Ack when the given message ID is
// not currently in-flight on this queue.
var ErrMessageNotFound = errors.New("message not found or not in-flight")

// Ack acknowledges successful processing of an in-flight message, removing
// it from the queue permanently.
func (q *Queue) Ack(messageID string) error {
	req := ackRequest{messageID: messageID, resp: make(chan error)}
	q.ackCh <- req
	return <-req.resp
}

// Close cancels the dispatcher goroutine (run) and blocks until it has
// exited.
// TODO: add bounds to ensure close cannot be called twice successfully, 
// and that no other methods are called after Close.
func (q *Queue) Close() {
	q.cancel()
	<-q.done
}


