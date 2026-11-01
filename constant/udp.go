package constant

import "time"

// UDPQueuePolicy is an optional capability of an outbound / packet connection.
// The tunnel applies it to its UDP sender queue, before calling WriteTo.
// It does not bound time spent inside WriteTo or the transport's own queues.
type UDPQueuePolicy interface {
	UDPMaxQueueDelay() time.Duration
}
