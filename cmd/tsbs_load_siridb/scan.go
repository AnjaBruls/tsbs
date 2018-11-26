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
	data      []byte
	metricCnt uint16
}

type batch struct {
	series []byte
	cnt    int
}

func (b *batch) Len() int {
	return b.cnt
}

func (b *batch) Append(item *load.Point) {
	that := item.Data.(*point)
	b.series = append(b.series, that.data...)
	b.cnt += int(that.metricCnt)
}

type factory struct{}

func (f *factory) New() load.Batch {
	serie := make([]byte, 0)
	serie = append([]byte{byte(253)}, serie...)
	return &batch{
		series: serie,
		cnt:    0,
	}
}

type decoder struct {
	buf []byte
	len uint32
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

	d.len += uint32(n)
	d.buf = append(d.buf, buf[:n]...)
	return n
}

func (d *decoder) Decode(bf *bufio.Reader) *load.Point {
	if d.len < HeaderSize {
		if n := d.Read(bf); n == 0 {
			return nil
		}
	}

	lengthData := binary.LittleEndian.Uint32(d.buf[:4])
	metricCnt := binary.LittleEndian.Uint16(d.buf[4:6])

	total := lengthData + HeaderSize
	for d.len < total {
		if n := d.Read(bf); n == 0 {
			return nil
		}
	}

	data := d.buf[HeaderSize:total]

	d.buf = d.buf[total:]
	d.len -= total

	return load.NewPoint(&point{
		data:      data,
		metricCnt: metricCnt,
	})
}
