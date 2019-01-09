package common

import (
	"time"

	"../serialize"
)

// SimulatorConfig is an interface to create a Simulator from a time.Duration
type SimulatorConfig interface {
	NewSimulator(time.Duration, uint64) Simulator
}

// Simulator simulates a use case.
type Simulator interface {
	Finished() bool
	Next(*serialize.Point) bool
	Fields() map[string][][]byte
}

// SimulatedMeasurement simulates one measurement (e.g. Redis for DevOps).
type SimulatedMeasurement interface {
	Tick(time.Duration)
	ToPoint(*serialize.Point)
}
