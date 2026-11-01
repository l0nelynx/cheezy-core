package constant

import "time"

// UDPRoute describes the current child of a logical, multi-destination socket.
type UDPRoute struct {
	ID             string `json:"id"`
	Destination    string `json:"destination"`
	Chains         Chain  `json:"chains"`
	ProviderChains Chain  `json:"providerChains"`
}

type UDPRouteReporter interface {
	UDPRoutes() []UDPRoute
}

// UDPQueuePolicy is an optional capability of an outbound / packet connection.
// The tunnel applies it to its UDP sender queue, before calling WriteTo.
// It does not bound time spent inside WriteTo or the transport's own queues.
type UDPQueuePolicy interface {
	UDPMaxQueueDelay() time.Duration
}
