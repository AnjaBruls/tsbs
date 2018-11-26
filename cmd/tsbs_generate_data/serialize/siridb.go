package serialize

import (
	"fmt"
	"io"
)

// TimescaleDBSerializer writes a Point in a serialized form for TimescaleDB
type SiriDBSerializer struct{}

// Serialize writes Point p to the given Writer w, so it can be
// loaded by the TimescaleDB loader. The format is CSV with two lines per Point,
// with the first row being the tags and the second row being the field values.
//
// e.g.,
// tags,<tag1>,<tag2>,<tag3>,...
// <measurement>,<timestamp>,<field1>,<field2>,<field3>,...
func (s *SiriDBSerializer) Serialize(p *Point, w io.Writer) error {

	// Tag row first, prefixed with name 'tags'
	buf := make([]byte, 0, 1024) // 512?
	buf = append(buf, p.measurementName...)
	buf = append(buf, ';')
	for i, v := range p.tagValues {
		if i != 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, p.tagKeys[i]...)
		buf = append(buf, '=')
		buf = append(buf, v...)
	}
	buf = append(buf, ';')
	buf = append(buf, []byte(fmt.Sprintf("%d", p.timestamp.UTC().UnixNano()))...)
	buf = append(buf, ';')
	for i, v := range p.fieldValues {
		if i > 0 {
			buf = append(buf, ',')
		}

		buf = append(buf, p.fieldKeys[i]...)
		buf = append(buf, '=')
		buf = fastFormatAppend(v, buf)
		buf = append(buf, ':')
		buf = append(buf, []byte(typeNameForSiri(v))...)

	}
	buf = append(buf, '\n')
	_, err := w.Write(buf)
	// fmt.Fprintf(os.Stderr, "%s\n", p.timestamp) // int64????
	return err
}

func typeNameForSiri(v interface{}) string {
	switch v.(type) {
	case int, int64:
		return "int"
	case float32, float64:
		return "float"
	case bool:
		return "boolean"
	case []byte:
		return "byte"
	case string:
		return "string"
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}

// package serialize

// import (
// 	"bytes"
// 	"encoding/binary"
// 	"fmt"
// 	"io"
// 	"log"
// 	"os"
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
// 	name := make([]byte, 0, 256) // 512?

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
// 	buf := bytes.NewBuffer(name)
// 	nameString := buf.String()

// 	fieldkey := make([]string, 0, 256) // 512?
// 	for _, v := range p.fieldKeys {
// 		buf := bytes.NewBuffer(v)
// 		fieldkey = append(fieldkey, buf.String())
// 	}

// 	fieldvalue := make([]interface{}, 0, 256)
// 	metricCount := 0
// 	for _, v := range p.fieldValues {
// 		fieldvalue = append(fieldvalue, v)
// 		metricCount++
// 	}

// 	var line = make([]byte, 0)
// 	for i, _ := range fieldvalue {

// 		ts, _ := strconv.ParseInt(fmt.Sprintf("%d", p.timestamp.UTC().UnixNano()), 10, 64)

// 		keyString := nameString + "   Field: " + fieldkey[i]
// 		value := fieldvalue[i]
// 		key, err := qpack.Pack(keyString)
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		data, err := qpack.Pack([][]interface{}{{ts, value}})
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		line = append(line, key...)
// 		line = append(line, data...)
// 	}

// 	lenData := make([]byte, 4)
// 	binary.LittleEndian.PutUint32(lenData, uint32(len(line)))

// 	lenMetrics := make([]byte, 2)
// 	binary.LittleEndian.PutUint16(lenMetrics, uint16(metricCount))

// 	line = append(lenMetrics, line...)
// 	line = append(lenData, line...)

// 	fmt.Fprintf(os.Stderr, "%v\n", len(line)) // int64????

// 	_, err := w.Write(line)

// 	return err
// }
