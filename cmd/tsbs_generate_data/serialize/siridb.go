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

// package serialize

// import (
// 	"encoding/binary"
// 	"fmt"
// 	"io"
// 	"log"
// 	"strconv"

// 	"github.com/transceptor-technology/go-qpack"
// )

// // TimescaleDBSerializer writes a Point in a serialized form for TimescaleDB
// type SiriDBSerializer struct{}

// // Serialize writes Point p to the given Writer w, so it can be
// // loaded by the TimescaleDB loader. The format is CSV with two lines per Point,
// // with the first row being the tags and the second row being the field values.
// //
// // e.g.,
// // tags,<tag1>,<tag2>,<tag3>,...
// // <measurement>,<timestamp>,<field1>,<field2>,<field3>,...
// func (s *SiriDBSerializer) Serialize(p *Point, w io.Writer) error {

// 	// Tag row first, prefixed with name 'tags'
// 	name := make([]byte, 0, 1024) // 512?

// 	name = append(name, []byte("Measurement name: ")...)
// 	name = append(name, p.measurementName...)
// 	name = append(name, ' ')
// 	name = append(name, []byte("Tags: ")...)
// 	for i, v := range p.tagValues {
// 		if i != 0 {
// 			name = append(name, ',')
// 		}
// 		name = append(name, p.tagKeys[i]...)
// 		name = append(name, '=')
// 		name = append(name, v...)
// 	}
// 	var err error
// 	metricCount := 0
// 	line := make([]byte, 0, 1024)
// 	for i, value := range p.fieldValues {
// 		ts, _ := strconv.ParseInt(fmt.Sprintf("%d", p.timestamp.UTC().UnixNano()), 10, 64)

// 		key := []byte("  Field: ")
// 		key = append(key, p.fieldKeys[i]...)
// 		// fmt.Fprintf(os.Stderr, "key: %s\n", key) // int64????
// 		data, err := qpack.Pack([]interface{}{ts, value})
// 		if err != nil {
// 			log.Fatal(err)
// 		}

// 		line = append(data, line...)
// 		line = append(key, line...)

// 		lenData := make([]byte, 2)
// 		binary.LittleEndian.PutUint16(lenData, uint16(len(data)))
// 		line = append(lenData, line...)

// 		lenKey := make([]byte, 2)
// 		binary.LittleEndian.PutUint16(lenKey, uint16(len(key)))
// 		line = append(lenKey, line...)

// 		metricCount++
// 	}
// 	lenMetrics := make([]byte, 2)
// 	binary.LittleEndian.PutUint16(lenMetrics, uint16(metricCount))

// 	lenName := make([]byte, 2)
// 	binary.LittleEndian.PutUint16(lenName, uint16(len(name)))

// 	line = append(name, line...)
// 	line = append(lenName, line...)
// 	line = append(lenMetrics, line...)

// 	_, err = w.Write(line)
// 	return err
// }
