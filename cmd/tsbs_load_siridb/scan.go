// package main

// import (
// 	"bufio"
// 	"fmt"
// 	"strconv"
// 	"strings"

// 	"../../load"
// )

// // point is a single row of data keyed by which hypertable it belongs
// type data struct {
// 	line []string
// }

// type point struct {
// 	pointName   string
// 	ts          int64
// 	fieldkeys   []string
// 	fieldvalues []interface{}
// }

// type batch struct {
// 	serie     map[string][][]interface{}
// 	batchCnt  int
// 	metricCnt uint64
// }

// func (b *batch) Len() int {
// 	return b.batchCnt
// }

// func (b *batch) Append(item *load.Point) {
// 	that := item.Data.(*point)

// 	for i := 0; i < len(that.fieldvalues); i++ {
// 		keyString := that.pointName + "   Field: " + that.fieldkeys[i]
// 		b.serie[keyString] = append(b.serie[keyString], []interface{}{that.ts, that.fieldvalues[i]})
// 	}
// 	b.metricCnt += uint64(len(that.fieldvalues))
// 	b.batchCnt++
// }

// type factory struct{}

// func (f *factory) New() load.Batch {
// 	return &batch{
// 		serie:     map[string][][]interface{}{},
// 		batchCnt:  0,
// 		metricCnt: 0,
// 	}
// }

// type decoder struct {
// 	scanner *bufio.Scanner
// }

// func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
// 	data := &data{}
// 	ok := d.scanner.Scan()

// 	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
// 		return nil
// 	} else if !ok {
// 		fatal("scan error: %v", d.scanner.Err())
// 		return nil
// 	}
// 	data.line = strings.SplitN(d.scanner.Text(), ";", 4)
// 	fields := strings.Split(data.line[3], ",")

// 	fieldkeys, fieldvalues := []string{}, []interface{}{}
// 	for _, field := range fields {
// 		splitfield := strings.Split(field, "=")
// 		key := splitfield[0]
// 		value := strings.Split(splitfield[1], ":")

// 		valueConverted, err := typeConversion(value[1], value[0])
// 		if err != nil {
// 			fatal(err)
// 		}
// 		fieldkeys, fieldvalues = append(fieldkeys, key), append(fieldvalues, valueConverted)
// 	}

// 	ts, err := strconv.ParseInt(data.line[2], 10, 64)
// 	if err != nil {
// 		fatal(err)
// 	}

// 	return load.NewPoint(&point{
// 		pointName:   "Measurement name: " + data.line[0] + "   Tags: " + data.line[1],
// 		ts:          ts,
// 		fieldkeys:   fieldkeys,
// 		fieldvalues: fieldvalues,
// 	})
// }

// func typeConversion(datatype string, datapoint string) (interface{}, error) {
// 	switch datatype { // contains data type description
// 	case "int":
// 		return strconv.ParseInt(datapoint, 10, 64)
// 	case "float":
// 		return strconv.ParseFloat(datapoint, 64)
// 	case "boolean":
// 		return strconv.ParseBool(datapoint)
// 	case "byte":
// 		return datapoint, nil
// 	case "string":
// 		return datapoint, nil
// 	default:
// 		panic(fmt.Sprintf("unknown field type for %T", datapoint))
// 	}
// }

// package main

// import (
// 	"bufio"
// 	"encoding/binary"
// 	"io"
// 	"log"

// 	"../../load"
// )

// // HeaderSize if the size of a package header.
// const HeaderSize = 6

// type point struct {
// 	data      []byte
// 	metricCnt uint64
// }

// type batch struct {
// 	series    []byte
// 	batchCnt  int
// 	metricCnt uint64
// }

// func (b *batch) Len() int {
// 	return b.batchCnt
// }

// func (b *batch) Append(item *load.Point) {
// 	that := item.Data.(*point)
// 	b.series = append(b.series, that.data...)
// 	b.batchCnt++
// 	b.metricCnt += that.metricCnt
// }

// type factory struct{}

// func (f *factory) New() load.Batch {
// 	return &batch{
// 		series:    []byte{byte(253)},
// 		batchCnt:  0,
// 		metricCnt: 0,
// 	}
// }

// type decoder struct {
// 	buf []byte
// 	len uint32
// }

// func (d *decoder) Read(bf *bufio.Reader) int {
// 	buf := make([]byte, 8192)
// 	n, err := bf.Read(buf)
// 	if err == io.EOF {
// 		return n
// 	}
// 	if err != nil {
// 		log.Fatal(err.Error())
// 	}

// 	d.len += uint32(n)
// 	d.buf = append(d.buf, buf[:n]...)
// 	return n
// }

// func (d *decoder) Decode(bf *bufio.Reader) *load.Point {

// 	if d.len < HeaderSize {
// 		if n := d.Read(bf); n == 0 {
// 			return nil
// 		}
// 	}

// 	lengthData := binary.LittleEndian.Uint32(d.buf[:4])
// 	metricCnt := binary.LittleEndian.Uint16(d.buf[4:6])

// 	total := lengthData + HeaderSize
// 	for d.len < total {
// 		if n := d.Read(bf); n == 0 {
// 			return nil
// 		}
// 	}

// 	data := d.buf[HeaderSize:total]

// 	d.buf = d.buf[total:]
// 	d.len -= total

// 	return load.NewPoint(&point{
// 		data:      data,
// 		metricCnt: uint64(metricCnt),
// 	})
// }

package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"

	"../../load"
)

// HeaderSize if the size of a package header.
const HeaderSize = 6

type point struct {
	key     [][]byte
	data    [][]byte
	dataCnt uint64
}

type batch struct {
	series    map[string][]byte // series    []byte
	batchCnt  int
	metricCnt uint64
}

func (b *batch) Len() int {
	return b.batchCnt
}

func (b *batch) Append(item *load.Point) {
	that := item.Data.(*point)
	for i, _ := range that.data {
		key := string(that.key[i])
		if len(b.series[key]) == 0 {
			b.series[key] = append(b.series[key], byte(252)) // qpack: open array
		}
		b.series[key] = append(b.series[key], that.data[i]...)
	}
	b.metricCnt += that.dataCnt
	b.batchCnt++
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &batch{
		series:    map[string][]byte{}, // []byte{byte(253)},
		batchCnt:  0,
		metricCnt: 0,
	}
}

type decoder struct {
	buf []byte
	len uint16
}

func (d *decoder) Read(bf *bufio.Reader) int {
	buf := make([]byte, 8192)
	n, err := bf.Read(buf)
	if err == io.EOF {
		return n
	}
	if err != nil {
		log.Fatal(err.Error())
	}

	d.len += uint16(n)
	d.buf = append(d.buf, buf[:n]...)
	return n
}

func (d *decoder) Decode(bf *bufio.Reader) *load.Point {

	if d.len < HeaderSize {
		if n := d.Read(bf); n == 0 {
			return nil
		}
	}
	valueCnt := binary.LittleEndian.Uint16(d.buf[:2])
	d.buf = d.buf[2:]
	d.len -= 2

	var key [][]byte
	var data [][]byte
	for i := 0; uint16(i) < valueCnt; i++ {
		if d.len < HeaderSize {
			if n := d.Read(bf); n == 0 {
				return nil
			}
		}
		lengthKey := binary.LittleEndian.Uint16(d.buf[:2])
		lengthData := binary.LittleEndian.Uint16(d.buf[2:4])

		total := lengthData + HeaderSize - 2
		for d.len < total {
			if n := d.Read(bf); n == 0 {
				return nil
			}
		}
		key = append(key, d.buf[4:lengthKey+4])
		data = append(data, d.buf[lengthKey+4:total])

		d.buf = d.buf[total:]
		d.len -= total
	}

	return load.NewPoint(&point{
		key:     key,
		data:    data,
		dataCnt: uint64(valueCnt),
	})
}