package domain

type RLSViolationCount struct {
	ViolationType string
	Count         int64
}

type OperationalStats struct {
	DegradedInstances         int64
	OldestDegradedAgeSeconds  float64
	OldestReadyTaskAgeSeconds float64
	InstancesByQueue          map[string]int64
}
