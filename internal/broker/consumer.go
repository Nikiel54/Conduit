package broker

import (
	"context"
	"time"
)

// inFlightEntry pairs a dequeued message with the timer that will fire if
// it isn't acked in time.
type inFlightEntry struct {
	message *Message
	timer   *time.Timer
}

// timeoutEvent is sent on a queue's timeoutCh when a visibility timeout fires.
type timeoutEvent struct {
	messageID string
}

// startVisibilityTimer arms a timer for a freshly dequeued message. If the
// timer fires before run() has processed an Ack (i.e., before
// it's removed from inFlight), the timer's callback sends a timeoutEvent
// back to run() over q.timeoutCh -- which then either redelivers msg or
// moves it to the DLQ, depending on DeliveryCount vs MaxRetries.
func (q *Queue) startVisibilityTimer(ctx context.Context, msg *Message, visibilityTimeout time.Duration) *inFlightEntry {
	timer := time.AfterFunc(visibilityTimeout, func() {
		select {
		case q.timeoutCh <- timeoutEvent{messageID: msg.ID}:
		case <-ctx.Done():
		}
	})

	return &inFlightEntry{message: msg, timer: timer}
}


