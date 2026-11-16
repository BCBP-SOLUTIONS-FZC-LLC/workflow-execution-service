package domain

type RLSViolationCount struct {
	ViolationType string
	Count         int64
}

type OperationalStats struct {
	DegradedInstances         int64
	OldestDegradedAgeSeconds  float64
	OldestReadyTaskAgeSeconds float64
	// OldestReadyConnectorTaskAgeSeconds is the same age over connector tasks only; 0 when none is READY.
	OldestReadyConnectorTaskAgeSeconds float64
	InstancesByQueue                   map[string]int64
}
