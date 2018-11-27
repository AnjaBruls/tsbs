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
			if _, err := p.client.Insert(batch.serie, uint16(writeTimeout)); err != nil {
				fatal(err)
			}
			if logBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := batch.batchCnt
				fmt.Printf("BATCH: batchsize %d insert rate %f/sec (took %v)\n", batchSize, float64(batchSize)/float64(took.Seconds()), took)
			}
		}
	} else {
		fatal("not even a single server is connected...hoi")
	}
	metricCount = uint64(batch.metricCnt)
	batch.serie = map[string][][]interface{}{}
	batch.batchCnt = 0
	batch.metricCnt = 0
	return metricCount, 0
}

// package main

// import (
// 	"fmt"
// 	"strconv"
// 	"strings"
// 	"time"

// 	"../../../go-siridb-connector"
// 	"../../load"
// )

// type processor struct {
// 	client *siridb.Client
// }

// func (p *processor) Init(numWorker int, doLoad bool) {
// 	hostlist := [][]interface{}{}
// 	if doLoad {
// 		listhostports := strings.Split(hosts, ",")

// 		for _, hostport := range listhostports {
// 			host_port := strings.Split(hostport, ":")
// 			host := host_port[0]
// 			port, err := strconv.ParseInt(host_port[1], 10, 0)
// 			if err != nil {
// 				fatal(err)
// 			}
// 			hostlist = append(hostlist, []interface{}{host, int(port)})
// 		}
// 	}

// 	p.client = siridb.NewClient(
// 		dbUser,                // username
// 		dbPass,                // password
// 		loader.DatabaseName(), // database
// 		hostlist,              // siridb server(s)
// 		nil,                   // optional log channel
// 	)
// 	p.client.Connect()

// }

// func (p *processor) Close(doLoad bool) {
// 	if doLoad {

// 		p.client.Close()
// 	}
// }

// func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (metricCount, rows uint64) {
// 	batch := b.(*batch)
// 	if p.client.IsConnected() {
// 		if doLoad {
// 			start := time.Now()
// 			if _, err := p.client.InsertBin(batch.series, uint16(writeTimeout)); err != nil {
// 				fatal(err)
// 			}
// 			if logBatches {
// 				now := time.Now()
// 				took := now.Sub(start)
// 				batchSize := batch.batchCnt
// 				fmt.Printf("BATCH: batchsize %d insert rate %f/sec (took %v)\n", batchSize, float64(batchSize)/float64(took.Seconds()), took)
// 			}
// 		}
// 	} else {
// 		fatal("not even a single server is connected...hoi")
// 	}
// 	metricCount = uint64(batch.metricCnt)
// 	serie := make([]byte, 0)
// 	serie = append([]byte{byte(253)}, serie...)
// 	batch.series = serie
// 	batch.batchCnt = 0
// 	batch.metricCnt = 0
// 	return metricCount, 0
// }
