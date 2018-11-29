// tsbs_run_queries_timescaledb speed tests TimescaleDB using requests from stdin or file
//
// It reads encoded Query objects from stdin or file, and makes concurrent requests
// to the provided PostgreSQL/TimescaleDB endpoint.
// This program has no knowledge of the internals of the endpoint.
package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"../../query"
	siridb "github.com/SiriDB/go-siridb-connector"
	_ "github.com/lib/pq"
)

const (
	account       = "sa"
	password      = "siri"
	timePrecision = "ns"
	bufferSize    = 1024
	durationNum   = "1w" // SHOULD BE SHORTER
	durationLog   = "1d"
)

// Program option vars:
var (
	hosts        string
	writeTimeout int
	datapath     string
	dbUser       string
	dbPass       string
	showExplain  bool
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	runner = query.NewBenchmarkRunner()

	flag.StringVar(&datapath, "datapath", "../../../../../tmp/siridb-data.gz", "Path to the zipped file in SiriDB format ")
	flag.StringVar(&dbUser, "dbuser", "iris", "Username to enter SiriDB")
	flag.StringVar(&dbPass, "dbpass", "siri", "Password to enter SiriDB")
	flag.StringVar(&hosts, "hosts", "localhost:9000", "Comma separated list of SiriDB hosts in a cluster.")
	flag.IntVar(&writeTimeout, "write-timeout", 10, "Write timeout.")

	flag.BoolVar(&showExplain, "show-explain", false, "Print out the EXPLAIN output for sample query")

	if showExplain { //??
		runner.ResetLimit(1)
	}

	flag.Parse()
}

func main() {
	runner.Run(&query.TimescaleDBPool, newProcessor)
}

type queryExecutorOptions struct {
	showExplain   bool
	debug         bool
	printResponse bool
}

type processor struct {
	client *siridb.Client
	opts   *queryExecutorOptions
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(numWorker int, doLoad bool) {
	hostlist := [][]interface{}{}
	listhostports := strings.Split(hosts, ",")

	for _, hostport := range listhostports {
		host_port := strings.Split(hostport, ":")
		host := host_port[0]
		port, err := strconv.ParseInt(host_port[1], 10, 0)
		if err != nil {
			log.Fatal(err)
		}
		hostlist = append(hostlist, []interface{}{host, int(port)})
	}

	p.client = siridb.NewClient(
		dbUser,                // username
		dbPass,                // password
		runner.DatabaseName(), // database
		hostlist,              // siridb server(s)
		nil,                   // optional log channel
	)
	p.client.Connect()

	p.opts = &queryExecutorOptions{
		showExplain:   showExplain,
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}

}

func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.client.Close()
	}
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	// No need to run again for EXPLAIN
	if isWarm && p.opts.showExplain {
		return nil, nil
	}
	tq := q.(*query.SiriDB)

	start := time.Now()
	qry := string(tq.SqlQuery)
	defer p.client.Close()
	var res interface{}
	var err error
	if p.client.IsConnected() {
		if res, err = p.client.Query(qry, uint16(writeTimeout)); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("not even a single server is connected...hoi")
	}

	if p.opts.debug {
		fmt.Println(qry)
	}

	if p.opts.printResponse {
		fmt.Printf("Query result: %s\n", res)
	}

	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, err
}
