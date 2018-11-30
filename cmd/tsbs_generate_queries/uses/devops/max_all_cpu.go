package devops

import (
	"../../../../query"
	"../../utils"
)

// MaxAllCPU contains info for filling in a query.Query for "max all" queries
type MaxAllCPU struct {
	core  utils.DevopsGenerator
	hosts int
}

// NewMaxAllCPU produces a new function that produces a new AllMaxCPU
func NewMaxAllCPU(hosts int) utils.QueryFillerMaker {
	return func(core utils.DevopsGenerator) utils.QueryFiller {
		return &MaxAllCPU{
			core:  core,
			hosts: hosts,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *MaxAllCPU) Fill(q query.Query) query.Query {
	fc, ok := d.core.(MaxAllFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.MaxAllCPU(q, d.hosts)
	return q
}
