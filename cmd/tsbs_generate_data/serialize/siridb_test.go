package serialize

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"testing"

	qpack "github.com/transceptor-technology/go-qpack"
)

func TestSiriDBSerializerSerialize(t *testing.T) {
	type output struct {
		seriename []string
		value     [][]interface{}
	}
	cases := []struct {
		desc       string
		inputPoint *Point
		want       output
	}{
		{
			desc:       "a regular Point",
			inputPoint: testPointDefault,
			want: output{
				seriename: []string{"cpu|hostname=host_0,region=eu-west-1,datacenter=eu-west-1b|usage_guest_nice"},
				value:     [][]interface{}{{1451606400000000000, 38.24311829}},
			},
		},
		{
			desc:       "a regular Point using int as value",
			inputPoint: testPointInt,
			want: output{
				seriename: []string{"cpu|hostname=host_0,region=eu-west-1,datacenter=eu-west-1b|usage_guest"},
				value:     [][]interface{}{{1451606400000000000, 38}},
			},
		},
		{
			desc:       "a regular Point with multiple fields",
			inputPoint: testPointMultiField,
			want: output{
				seriename: []string{
					"cpu|hostname=host_0,region=eu-west-1,datacenter=eu-west-1b|big_usage_guest",
					"cpu|hostname=host_0,region=eu-west-1,datacenter=eu-west-1b|usage_guest",
					"cpu|hostname=host_0,region=eu-west-1,datacenter=eu-west-1b|usage_guest_nice",
				},
				value: [][]interface{}{
					{1451606400000000000, 5000000000},
					{1451606400000000000, 38},
					{1451606400000000000, 38.24311829},
				},
			},
		},
		{
			desc:       "a Point with no tags",
			inputPoint: testPointNoTags,
			want: output{
				seriename: []string{"cpu||usage_guest_nice"},
				value:     [][]interface{}{{1451606400000000000, 38.24311829}},
			},
		},
	}

	ps := &SiriDBSerializer{}
	d := &decoder{}
	for _, c := range cases {
		b := new(bytes.Buffer)
		ps.Serialize(c.inputPoint, b)
		br := bufio.NewReader(bytes.NewReader(b.Bytes()))
		key, data := d.deSerializeSiriDB(br)

		for i, k := range key {
			if got := k; got != c.want.seriename[i] {
				t.Errorf("%s \nOutput incorrect: \nWant: '%s' \nGot:  '%s'", c.desc, c.want.seriename[i], got)
			}

			var unpacked interface{}
			var err error
			if unpacked, err = qpack.Unpack(data[i], 1); err != nil {
				t.Errorf("%s", err)
			}

			switch v := unpacked.(type) {
			case []interface{}:
				for j, got := range v {
					if got != c.want.value[i][j] {
						t.Errorf("%s \nOutput incorrect: \nWant: '%s' \nGot:  '%s'", c.desc, c.want.value[i][j], got)
					}
				}
			default:
				t.Errorf("Qpack returned the incorrect type: %T", v)

			}

		}
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

func (d *decoder) deSerializeSiriDB(bf *bufio.Reader) ([]string, [][]byte) {
	if d.len < 4 {
		if n := d.Read(bf); n == 0 {
			return nil, nil
		}
	}
	valueCnt := binary.LittleEndian.Uint16(d.buf[:2])
	nameCnt := binary.LittleEndian.Uint16(d.buf[2:4])

	d.buf = d.buf[4:]
	d.len -= 4

	if d.len < nameCnt {
		if n := d.Read(bf); n == 0 {
			return nil, nil
		}
	}

	name := d.buf[:nameCnt]

	d.buf = d.buf[nameCnt:]
	d.len -= nameCnt

	key := make([]string, 0)
	data := make([][]byte, 0)
	for i := 0; uint16(i) < valueCnt; i++ {
		if d.len < 4 {
			if n := d.Read(bf); n == 0 {
				return nil, nil
			}
		}
		lengthKey := binary.LittleEndian.Uint16(d.buf[:2])
		lengthData := binary.LittleEndian.Uint16(d.buf[2:4])

		total := lengthData + lengthKey + 4
		for d.len < total {
			if n := d.Read(bf); n == 0 {
				return nil, nil
			}
		}

		key = append(key, string(name)+string(d.buf[4:lengthKey+4]))
		data = append(data, d.buf[lengthKey+4:total])

		d.buf = d.buf[total:]
		d.len -= total
	}
	return key, data
}

func TestSiriDBSerializerSerializeErr(t *testing.T) {
	p := testPointMultiField
	s := &SiriDBSerializer{}
	err := s.Serialize(p, &errWriter{})
	if err == nil {
		t.Errorf("no error returned when expected")
	} else if err.Error() != errWriterAlwaysErr {
		t.Errorf("unexpected writer error: %v", err)
	}
}
