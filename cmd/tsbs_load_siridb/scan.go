package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"

	"../../load"
)

type point struct {
	data []byte
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
	b.cnt++
	// fmt.Println(b.cnt)
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &batch{
		series: make([]byte, 0, 10000),
		cnt:    0,
	}
}

type decoder struct {
}

const tagsPrefix = "tags"

func (d *decoder) Decode(bf *bufio.Reader) *load.Point {
	var err error
	var n int
	lenData := make([]byte, 4)
	_, err = bf.Read(lenData)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		log.Fatal(err.Error())
	}
	lenSlice := make([]byte, 4)
	_, err = bf.Read(lenSlice)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		log.Fatal(err.Error())
	}

	lengthData := binary.LittleEndian.Uint32(lenData)
	lengthSlice := binary.LittleEndian.Uint32(lenSlice)
	data := make([]byte, lengthSlice)
	n, err = bf.Read(data)
	// fmt.Println(data)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		log.Fatal(err.Error())
	}
	if uint32(n) < lengthSlice {
		var p int
		for uint32(n) < lengthSlice {
			part := make([]byte, lengthSlice-uint32(n))
			p, err = bf.Read(part)

			data = append(data[:n], part...)

			if lengthData > 8192 || lengthSlice > 8192 {
				log.Fatal("length too long")
			}

			if err == io.EOF {
				return nil
			}
			if err != nil {
				log.Fatal(err.Error())
			}
			n += p
		}

	}
	return load.NewPoint(&point{
		data: data[:lengthData],
	})
}

// func (d *decoder) Decode(r *bufio.Reader) *load.Point {
// 	l := make([]byte, 4)
// 	n, err := r.Read(l)
// 	if n != 4 {
// 		log.Fatal("Reads more than 4 bytes")
// 	}
// 	if err == io.EOF {
// 		return nil
// 	}
// 	if err != nil {
// 		log.Fatal(err.Error())
// 	}

// 	length := binary.LittleEndian.Uint32(l)
// 	data := make([]byte, length)
// 	n, err = r.Read(data)
// 	if uint32(n) != length {
// 		fmt.Println(n, length, data)

// 		// log.Fatal("Reads more than expected")
// 	}
// 	if err == io.EOF {
// 		return nil
// 	}
// 	if err != nil {
// 		log.Fatal(err.Error())
// 	}
// 	fmt.Println(length)
// 	return load.NewPoint(&point{
// 		data: data,
// 	})
// }
