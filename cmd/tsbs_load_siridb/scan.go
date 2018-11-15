package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"

	"../../load"
)

// point is a single row of data keyed by which hypertable it belongs
type insertData struct {
	measurementName string
	tags            string
	fields          []string
	timestamp       string
}

type point struct {
	pointName   string
	ts          int64
	fieldkeys   []string
	fieldvalues []interface{}
}

type batch struct {
	serie map[string][][]interface{}
	cnt   int
}

func (b *batch) Len() int {
	return b.cnt
}

func (b *batch) Append(item *load.Point) {
	that := item.Data.(*point)

	for i := 0; i < len(that.fieldvalues); i++ {
		keyString := that.pointName + "   Field: " + that.fieldkeys[i]
		b.serie[keyString] = append(b.serie[keyString], []interface{}{that.ts, that.fieldvalues[i]})
		b.cnt++
	}
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &batch{
		serie: map[string][][]interface{}{},
		cnt:   0,
	}
}

type decoder struct {
	scanner *bufio.Scanner
}

const tagsPrefix = "tags"

func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	data := &insertData{}

	ok := d.scanner.Scan()

	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}
	raw := strings.SplitN(d.scanner.Text(), ";", 4)
	data.measurementName = raw[0]
	data.tags = raw[1]
	data.timestamp = raw[2]
	data.fields = strings.Split(raw[3], ",")

	fieldkeys, fieldvalues := []string{}, []interface{}{}
	for _, field := range data.fields {
		splitfield := strings.Split(field, "=")
		key := splitfield[0]
		value := strings.Split(splitfield[1], ":")

		valueConverted, err := typeConversion(value[1], value[0])
		if err != nil {
			fatal(err)
		}
		fmt.Println(key)
		fieldkeys, fieldvalues = append(fieldkeys, key), append(fieldvalues, valueConverted)
	}

	ts, err := strconv.ParseInt(data.timestamp, 10, 64)
	if err != nil {
		fatal(err)
	}

	return load.NewPoint(&point{
		pointName:   "Measurement name: " + data.measurementName + "   Tags: " + data.tags,
		ts:          ts,
		fieldkeys:   fieldkeys,
		fieldvalues: fieldvalues,
	})
}

func typeConversion(datatype string, datapoint string) (interface{}, error) {
	switch datatype { // contains data type description
	case "int":
		return strconv.ParseInt(datapoint, 10, 64)
	case "float":
		return strconv.ParseFloat(datapoint, 64)
	case "boolean":
		return strconv.ParseBool(datapoint)
	case "byte":
		return datapoint, nil
	case "string":
		return datapoint, nil
	default:
		panic(fmt.Sprintf("unknown field type for %T", datapoint))
	}
}
