package devops

import (
	"../../../../query"
	"../../utils"
)

// HighCPU produces a QueryFiller for the devops high-cpu cases
type HighCPU struct {
	core  utils.DevopsGenerator
	hosts int
}

// NewHighCPU produces a new function that produces a new HighCPU
func NewHighCPU(hosts int) utils.QueryFillerMaker {
	return func(core utils.DevopsGenerator) utils.QueryFiller {
		return &HighCPU{
			core:  core,
			hosts: hosts,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *HighCPU) Fill(q query.Query) query.Query {
	fc, ok := d.core.(HighCPUFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.HighCPUForHosts(q, d.hosts)
	return q
}
