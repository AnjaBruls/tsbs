package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"

	"github.com/timescale/tsbs/load"
)

type point struct {
	data    map[string][]byte
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
	for k, v := range that.data {
		if len(b.series[k]) == 0 {
			b.series[k] = append(b.series[k], byte(252)) // qpack: open array
		}
		b.series[k] = append(b.series[k], v...)
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
	if d.len < 4 {
		if n := d.Read(bf); n == 0 {
			return nil
		}
	}
	valueCnt := binary.LittleEndian.Uint16(d.buf[:2])
	nameCnt := binary.LittleEndian.Uint16(d.buf[2:4])

	d.buf = d.buf[4:]
	d.len -= 4

	if d.len < nameCnt {
		if n := d.Read(bf); n == 0 {
			return nil
		}
	}

	name := d.buf[:nameCnt]

	d.buf = d.buf[nameCnt:]
	d.len -= nameCnt

	data := make(map[string][]byte)
	for i := 0; uint16(i) < valueCnt; i++ {
		if d.len < 4 {
			if n := d.Read(bf); n == 0 {
				return nil
			}
		}
		lengthKey := binary.LittleEndian.Uint16(d.buf[:2])
		lengthData := binary.LittleEndian.Uint16(d.buf[2:4])

		total := lengthData + 4
		for d.len < total {
			if n := d.Read(bf); n == 0 {
				return nil
			}
		}
		fmt.Println(lengthKey)
		key := string(name) + string(d.buf[4:lengthKey+4])
		fmt.Println(total, d.len)
		data[key] = d.buf[lengthKey+4 : total]

		d.buf = d.buf[total:]
		d.len -= total
	}

	return load.NewPoint(&point{
		data:    data,
		dataCnt: uint64(valueCnt),
	})
}
