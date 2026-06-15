package broker

// handleTimeout runs when a message's visibility timeout expires without an Ack. 
// It either redelivers the message (if it hasn't hit MaxRetries yet) or adds to dlq.
func (q *Queue) handleTimeout(messageID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	entry, ok := q.inFlight[messageID]
	if !ok {
		// Already acked
		return
	}
	delete(q.inFlight, messageID)

	msg := entry.message
	if msg.DeliveryCount >= msg.MaxRetries {
		q.dlq = append(q.dlq, msg)
		return
	}

	q.pending[msg.Priority] = append(q.pending[msg.Priority], msg) // added to end; maybe I can add to front for immediate retries.
}


