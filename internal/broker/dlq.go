package broker

// DLQMessages returns a snapshot of every message currently in this
// queue's dead-letter queue, in the order they were moved there.
//
// There is currently no way to remove or "replay" messages from the DLQ
// back to the main queue. The spec calls a replay endpoint out as "a good
// resume bullet" — that's a natural admin-API addition for a later step,
// built on top of this same q.dlq slice.
func (q *Queue) DLQMessages() []*Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]*Message, len(q.dlq))
	for i, msg := range q.dlq {
		snapshot := *msg
		out[i] = &snapshot
	}
	return out
}
