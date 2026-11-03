package constant

import "time"

// OuterQUICQuality describes a single client-to-proxy carrier generation.
// RTT is in milliseconds and loss is a fraction of sent QUIC packets.
type OuterQUICQuality struct {
	Generation      uint64    `json:"generation"`
	SampledAt       time.Time `json:"sampledAt"`
	IntervalMS      float64   `json:"intervalMs"`
	MinRTTMS        float64   `json:"minRttMs"`
	SmoothedRTTMS   float64   `json:"smoothedRttMs"`
	DeviationMS     float64   `json:"deviationMs"`
	InflationMS     float64   `json:"inflationMs"`
	Sent            uint64    `json:"sent"`
	Received        uint64    `json:"received"`
	NetLossFraction *float64  `json:"netLossFraction,omitempty"`
	RTTUsed         bool      `json:"rttUsed"`
	Penalty         float64   `json:"penalty"`
}
type OuterQUICReporter interface{ OuterQUIC() *OuterQUICQuality }
