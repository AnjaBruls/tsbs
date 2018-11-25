package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"../../../go-siridb-connector"
	"../../load"
)

type processor struct {
	client *siridb.Client
}

func (p *processor) Init(numWorker int, doLoad bool) {
	hostlist := [][]interface{}{}
	if doLoad {
		listhostports := strings.Split(hosts, ",")

		for _, hostport := range listhostports {
			host_port := strings.Split(hostport, ":")
			host := host_port[0]
			port, err := strconv.ParseInt(host_port[1], 10, 0)
			if err != nil {
				fatal(err)
			}
			hostlist = append(hostlist, []interface{}{host, int(port)})
		}
	}

	p.client = siridb.NewClient(
		dbUser,                // username
		dbPass,                // password
		loader.DatabaseName(), // database
		hostlist,              // siridb server(s)
		nil,                   // optional log channel
	)
	p.client.Connect()

}

func (p *processor) Close(doLoad bool) {
	if doLoad {

		p.client.Close()
	}
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (metricCount, rows uint64) {
	batch := b.(*batch)
	if p.client.IsConnected() {
		if doLoad {
			start := time.Now()
			// fmt.Println(len(serie))
			if _, err := p.client.InsertBin(batch.series, uint16(writeTimeout)); err != nil {
				fatal(err)
			}
			if logBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := batch.cnt
				fmt.Printf("BATCH: batchsize %d insert rate %f/sec (took %v)\n", batchSize, float64(batchSize)/float64(took.Seconds()), took)
			}
		}
	} else {
		fatal("not even a single server is connected...hoi")
	}
	metricCount = uint64(batch.cnt)
	batch.series = make([]byte, 0, 10000)
	batch.cnt = 0
	return metricCount, 0
}
