package main

import (
	"bufio"
	"flag"
	"log"

	"../../load"
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
	hosts             string
	replicationFactor int
	writeTimeout      int
	datapath          string
	dbUser            string
	dbPass            string
	logBatches        bool
	dbpaths           string
)

// Global vars
var (
	loader *load.BenchmarkRunner
)

// allows for testing
var fatal = log.Fatal

// Parse args:
func init() {
	loader = load.GetBenchmarkRunner()
	flag.StringVar(&datapath, "datapath", "../../../../../tmp/siridb-data.gz", "Path to the zipped file in SiriDB format ")
	flag.StringVar(&dbUser, "dbuser", "iris", "Username to enter SiriDB")
	flag.StringVar(&dbPass, "dbpass", "siri", "Password to enter SiriDB")
	flag.StringVar(&dbpaths, "dbpaths", "../../../siridb-server/dbtest/dbpath0,../../../siridb-server/dbtest/dbpath1", "Database paths")

	flag.StringVar(&hosts, "hosts", "localhost:9000,localhost:9001", "Comma separated list of Cassandra hosts in a cluster.")

	flag.BoolVar(&logBatches, "log-batches", false, "Whether to time individual batches.")

	flag.IntVar(&replicationFactor, "replication-factor", 1, "Number of nodes that must have a copy of each key.")
	flag.IntVar(&writeTimeout, "write-timeout", 10, "Write timeout.")

	flag.Parse()

}

type benchmark struct{}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	return &load.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return &dbCreator{}
}

func main() {

	loader.RunBenchmark(&benchmark{}, load.SingleQueue)
}
