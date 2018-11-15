package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"../../load"
	siridb "github.com/SiriDB/go-siridb-connector"
)

type processor struct {
	client *siridb.Client
}

func (p *processor) Init(numWorker int, doLoad bool) {
	hostlist := [][]interface{}{}
	fmt.Println(doLoad)
	if doLoad {
		listhosts := strings.Split(hosts, ",")

		for _, host := range listhosts {
			hostport := strings.Split(host, ":")
			host := hostport[0]
			port, err := strconv.ParseInt(hostport[1], 10, 0)
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
			if _, err := p.client.Insert(batch.serie, uint16(writeTimeout)); err != nil {
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
	batch.serie = map[string][][]interface{}{}
	batch.cnt = 0
	return metricCount, 0
}
