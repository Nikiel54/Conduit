package broker

import (
	"sync"
	"time"
	"errors"
)

// enum priority types
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityMedium Priority = "medium"
	PriorityLow    Priority = "low"
)


// priorityOrder defines dequeue precedence: Dequeue always checks high
// first, then medium, then low.
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

	Priority Priority
	MaxRetries int

	// DeliveryCount is incremented every time this message is handed to a
	// consumer via Dequeue.
	DeliveryCount int

	// QueuedAt is the time this message was published, set once and never changed.
	QueuedAt time.Time
}


// inFlightEntry pairs a dequeued message with the timer that will fire if
// it isn't acked in time.
type inFlightEntry struct {
	message *Message
	timer   *time.Timer
}


type Queue struct {
	name string

	mu       sync.Mutex
	pending  map[Priority][]*Message // pending msgs, grouped by priority
	inFlight map[string]*inFlightEntry // in-flight msgs
	dlq      []*Message // dead msgs
}

// NewQueue creates an empty queue with the given name. Queues are created
// lazily by the Broker on first reference, so only when needed.
func NewQueue(name string) *Queue {
	return &Queue{
		name: name,
		pending: map[Priority][]*Message{
			PriorityHigh:   {},
			PriorityMedium: {},
			PriorityLow:    {},
		},
		inFlight: make(map[string]*inFlightEntry),
	}
}

// Enqueue adds a new message to the queue's pending buffer for its priority.
// Mutex is in place to protect the corresponding buffer from concurrent overwrites.
func (q *Queue) Enqueue(msg *Message) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.pending[msg.Priority] = append(q.pending[msg.Priority], msg)
}

// Dequeue removes and returns the next message according to strict
// priority order (high, then medium, then low; FIFO within each tier).
// It returns (nil, false) if every priority bucket is empty.
//
// A timer is started which expires if msg fails to Ack within the visibility timeout.
// Then the msg is either requeued or sent to dlq.
func (q *Queue) Dequeue(visibilityTimeout time.Duration) (*Message, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, priority := range priorityOrder {
		bucket := q.pending[priority]

		if len(bucket) == 0 {
			continue
		}

		msg := bucket[0] // pop top of this queue
		q.pending[priority] = bucket[1:]

		msg.DeliveryCount++

		timer := time.AfterFunc(visibilityTimeout, func() {
			q.handleTimeout(msg.ID)
		})
		q.inFlight[msg.ID] = &inFlightEntry{message: msg, timer: timer}

		snapshot := *msg
		return &snapshot, true
	}

	return nil, false
}


// ErrMessageNotFound is returned by Queue.Ack when the given message ID
// is not currently in-flight on this queue.
//
// For Step 2 (Tests 1-4), this distinction isn't exercised, so Ack returns
// this single error for "not currently in-flight" regardless of whether
// the ID never existed, was already acked, or expired and was redelivered/
// DLQ'd. internal/api/ack.go maps this to HTTP 404. If a future step needs
// the 409 case (e.g. a dedicated edge-case test), revisit this with a
// bounded "recently completed" set.
var ErrMessageNotFound = errors.New("message not found or not in-flight")


// Ack acknowledges successful processing of an in-flight message,
// removing it from the queue permanently.
func (q *Queue) Ack(messageID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	entry, ok := q.inFlight[messageID]
	if !ok {
		return ErrMessageNotFound
	}

	entry.timer.Stop()
	delete(q.inFlight, messageID)

	return nil
}
