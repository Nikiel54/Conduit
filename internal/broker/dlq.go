package broker

// dlqRequest asks the dispatcher for a snapshot of the queue's dead-letter
// messages.
type dlqRequest struct {
	resp chan []*Message
}

// DLQMessages returns a snapshot of every message currently in this queue's
// dead-letter queue, in the order they were moved there.
//
// There is currently no way to remove or "replay" messages from the DLQ
// back to the main queue. The spec calls a replay endpoint out as "a good
// resume bullet" -- that's a natural admin-API addition for a later step,
// built on top of this same dlq slice.
func (q *Queue) DLQMessages() []*Message {
	req := dlqRequest{resp: make(chan []*Message)}
	q.dlqCh <- req
	return <-req.resp
}

// dlqSnapshot copies every message in dlq, in order, so DLQMessages'
// caller can read them without racing run()'s future mutations to dlq.
func dlqSnapshot(dlq []*Message) []*Message {
	out := make([]*Message, len(dlq))
	for i, msg := range dlq {
		snapshot := *msg
		out[i] = &snapshot
	}
	return out
}


