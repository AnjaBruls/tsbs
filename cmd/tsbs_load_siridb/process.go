package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	siridb "github.com/SiriDB/go-siridb-connector"
	"github.com/timescale/tsbs/load"
	qpack "github.com/transceptor-technology/go-qpack"
)

type processor struct {
	client *siridb.Client
}

func (p *processor) Init(numWorker int, doLoad bool) {
	hostlist := [][]interface{}{}
	if doLoad {
		listhosts := strings.Split(hosts, ",")
		for _, hostport := range listhosts {
			x := strings.Split(hostport, ":")
			host := x[0]
			port, err := strconv.ParseInt(x[1], 10, 0)
			if err != nil {
				log.Fatal(err)
			}
			hostlist = append(hostlist, []interface{}{host, int(port)})
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
}

func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.client.Close()
	}
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (metricCount, rows uint64) {
	batch := b.(*batch)
	if doLoad {
		if p.client.IsConnected() {
			series := make([]byte, 0)
			series = append(series, byte(253)) // qpack: open map
			for k, v := range batch.series {
				key, err := qpack.Pack(k)
				if err != nil {
					log.Fatal(err)
				}
				series = append(series, key...)
				series = append(series, v...)
			}
			start := time.Now()
			if _, err := p.client.InsertBin(series, uint16(writeTimeout)); err != nil {
				fatal(err)
			}
			if logBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := batch.batchCnt
				fmt.Printf("BATCH: batchsize %d insert rate %f/sec (took %v)\n", batchSize, float64(batchSize)/float64(took.Seconds()), took)
			}
		} else {
			fatal("not even a single server is connected...")
		}
	}
	metricCount = uint64(batch.metricCnt)
	batch.series = map[string][]byte{} // []byte{byte(253)},
	batch.batchCnt = 0
	batch.metricCnt = 0
	return metricCount, 0
}
