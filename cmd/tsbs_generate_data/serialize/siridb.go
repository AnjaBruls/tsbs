package serialize

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/transceptor-technology/go-qpack"
)

// SiriDBSerializer writes a Point in a serialized form for TimescaleDB
type SiriDBSerializer struct{}

// Serialize writes Point p to the given Writer w, so it can be
// loaded by the TimescaleDB loader. The format is CSV with two lines per Point,
// with the first row being the tags and the second row being the field values.
//
// e.g.,
// tags,<tag1>,<tag2>,<tag3>,...
// <measurement>,<timestamp>,<field1>,<field2>,<field3>,...
func (s *SiriDBSerializer) Serialize(p *Point, w io.Writer) error {
	line := make([]byte, 4, 8192)
	line = append(line, p.measurementName...)
	line = append(line, '|')
	for i, v := range p.tagValues {
		if i != 0 {
			line = append(line, ',')
		}
		line = append(line, p.tagKeys[i]...)
		line = append(line, '=')
		line = append(line, v...)
	}

	lenName := len(line) - 4

	// Tag row first, prefixed with name 'tags'
	var err error
	metricCount := 0

	for i, value := range p.fieldValues {
		ts, _ := strconv.ParseInt(fmt.Sprintf("%d", p.timestamp.UTC().UnixNano()), 10, 64)

		key := make([]byte, 5, 256)
		key[4] = '|'

		key = append(key, p.fieldKeys[i]...)
		data, err := qpack.Pack([]interface{}{ts, value})
		if err != nil {
			log.Fatal(err)
		}

		binary.LittleEndian.PutUint16(key[0:], uint16(len(key)-4))
		binary.LittleEndian.PutUint16(key[2:], uint16(len(data)))

		line = append(line, key...)
		line = append(line, data...)

		metricCount++
	}

	binary.LittleEndian.PutUint16(line[0:], uint16(metricCount))
	binary.LittleEndian.PutUint16(line[2:], uint16(lenName))

	_, err = w.Write(line)
	return err
}
