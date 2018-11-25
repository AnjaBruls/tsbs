package serialize

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/transceptor-technology/go-qpack"
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
	name := make([]byte, 0, 256) // 512?

	name = append(name, []byte("Measurement name: ")...)
	name = append(name, p.measurementName...)
	name = append(name, ' ')
	name = append(name, []byte("Tags: ")...)
	for i, v := range p.tagValues {
		if i != 0 {
			name = append(name, ',')
		}
		name = append(name, p.tagKeys[i]...)
		name = append(name, '=')
		name = append(name, v...)
	}
	buf := bytes.NewBuffer(name)
	nameString := buf.String()

	fieldkey := make([]string, 0, 256) // 512?
	for _, v := range p.fieldKeys {
		buf := bytes.NewBuffer(v)
		fieldkey = append(fieldkey, buf.String())
	}

	fieldvalue := make([]interface{}, 0, 256)
	for _, v := range p.fieldValues {
		fieldvalue = append(fieldvalue, v)
	}

	// var serie = make(map[string][]interface{})
	var err error
	var data []byte

	for i, _ := range fieldvalue {
		var serie = make(map[string][][]interface{})
		ts, _ := strconv.ParseInt(fmt.Sprintf("%d", p.timestamp.UTC().UnixNano()), 10, 64)
		keyString := nameString + "   Field: " + fieldkey[i]
		value := fieldvalue[i]
		serie[keyString] = append(serie[keyString], []interface{}{ts, value})
		// serie[keyString] = [][]interface{}{ts, value}
		data, err = qpack.Pack(serie)
		if err != nil {
			log.Fatal(err)
		}

		length := uint32(len(data)) + uint32(8)
		var lengthSlice uint32
		if length < 1024 {
			lengthSlice = 1024
		} else if length < 2048 {
			lengthSlice = 2048
		} else if length < 4096 {
			lengthSlice = 4096
		} else if length < 8192 {
			lengthSlice = 8192
		}
		lenData := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenData, uint32(len(data)))
		lenSlice := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenSlice, uint32(lengthSlice-8))

		dataSlice := make([]byte, lengthSlice-length, lengthSlice)
		dataSlice = append(data, dataSlice...)
		dataSlice = append(lenSlice, dataSlice...)
		dataSlice = append(lenData, dataSlice...)

		// schrijf per metric

		// fmt.Fprintf(os.Stderr, "%s\n", data) // int64????

		// _, err = w.Write(l)
		_, err = w.Write(dataSlice)
	}

	return err
}

// 	l := make([]byte, 4)
// 	binary.LittleEndian.PutUint32(l, uint32(len(data)))
// 	data = append(l, data...)

// 	// fmt.Fprintf(os.Stderr, "%v, %v\n", len(data), data) // int64????

// 	// _, err = w.Write(l)
// 	_, err = w.Write(data)

// 	return err
// }
