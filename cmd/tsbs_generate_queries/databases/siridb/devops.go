package siridb

import (
	"fmt"
	"strings"
	"time"

	"../../../../query"
	"../../uses/devops"
)

// Devops produces SiriDB-specific queries for all the devops query types.
type Devops struct {
	*devops.Core
}

// NewDevops makes an Devops object ready to generate Queries.
func NewDevops(start, end time.Time, scale int) *Devops {
	return &Devops{devops.NewCore(start, end, scale)}
}

// GenerateEmptyQuery returns an empty query.SiriDB
func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewSiriDB()
}

func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf(".*(hostname=%s).*", s))
	}
	combinedHostnameClause := strings.Join(hostnameClauses, "|")
	return "/" + combinedHostnameClause + "/"
}

func (d *Devops) getHostWhereString(nhosts int) string {
	hostnames := d.GetRandomHosts(nhosts)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *Devops) getMetricWhereString(metrics []string) string {
	metricsClauses := []string{}
	for _, s := range metrics {
		metricsClauses = append(metricsClauses, fmt.Sprintf(".*(Field: %s$).*", s))
	}
	combinedMetricsClause := strings.Join(metricsClauses, "|")
	return "/" + combinedMetricsClause + "/"
}

const goTimeFmt = "2006-01-02 15:04:05Z"

//[[[[[[[[[[[[[[[[[[[[[[[[[[{{{{{{{MERGE PROBLEM}}}}}}}]]]]]]]]]]]]]]]]]]]]]]]]]]
//select max(5m) from /.*(Field: usage_guest$).*/ between '2016-01-01T12:00:00Z' and '2016-01-01T13:00:00Z' merge as 'max_per_metric' using max(1) ???
// select max(5m) from /.*(Field: usage_guest$).*/ & /.*(host_2).*|.*(host_5).*|.*(host_6).*/ before '2016-01-01T13:00:00Z' merge as 'max' using max(1)
//select max() from /.*(Field: usage_guest$).*|.*(Field: usage_guest_nice$).*|.*(Field: usage_user$).*/ & /.*(host_2).*|.*(host_5).*|.*(host_6).*/
//[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in psuedo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
// FROM cpu
// WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.RandWindow(timeRange)
	metrics := devops.GetCPUMetricsSlice(numMetrics)
	whereMetrics := d.getMetricWhereString(metrics)
	whereHosts := d.getHostWhereString(nHosts)

	humanLabel := fmt.Sprintf("SiriDB %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	siriql := fmt.Sprintf("select max(5m) from %s & %s between '%s'  and '%s' ", whereHosts, whereMetrics, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, siriql)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT time_bucket('1 minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.RandWindow(time.Hour)
	timeStr := interval.End.Format(goTimeFmt)

	where := fmt.Sprintf("between '%s' - 4m and '%s'", timeStr, timeStr)
	siriql := fmt.Sprintf("select max(1m) from %s", where)

	humanLabel := "SiriDB max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, siriql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in psuedo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	interval := d.Interval.RandWindow(devops.DoubleGroupByDuration)
	metrics := devops.GetCPUMetricsSlice(numMetrics)
	whereMetrics := d.getMetricWhereString(metrics)

	humanLabel := devops.GetDoubleGroupByLabel("SiriDB", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	siriql := fmt.Sprintf("select mean(1h) from %s between '%s'  and '%s' ", whereMetrics, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, siriql)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in psuedo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.RandWindow(devops.MaxAllDuration)

	whereMetrics := "/.*(Measurement name: cpu).*/"
	whereHosts := d.getHostWhereString(nHosts)

	humanLabel := devops.GetMaxAllLabel("SiriDB", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	siriql := fmt.Sprintf("select max(1h) from %s & %s between '%s'  and '%s' ", whereHosts, whereMetrics, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, siriql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	siriql := "select last() from *"
	humanLabel := "SiriDB last row per host"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, siriql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in psuedo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	var whereHosts string
	if nHosts == 0 {
		whereHosts = ""
	} else {
		whereHosts = "& " + d.getHostWhereString(nHosts)
	}
	interval := d.Interval.RandWindow(devops.HighCPUDuration)

	humanLabel := devops.GetHighCPULabel("Influx", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	siriql := fmt.Sprintf("select filter(> 90) from /.*(usage_user$).*/ %s between '%s' and '%s' ", whereHosts, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, siriql)
}

func (d *Devops) fillInQuery(qi query.Query, humanLabel, humanDesc, sql string) {
	q := qi.(*query.SiriDB)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.SqlQuery = []byte(sql)
}
