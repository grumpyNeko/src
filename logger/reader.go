package logger

import (
	"encoding/binary"
	"os"
)

type Reader struct {
	f        *os.File
	fileSize int64
	off      int64
	header   []byte
	buf      []byte // used as Payload in Record
	Record   Record
}

func NewReader(filepath string) Reader {
	f, err := os.OpenFile(filepath,  os.O_RDONLY, 0644)
	if err != nil {
		panic(err.Error())
	}
	info, err := f.Stat()
	if err != nil {
		panic(err.Error())
	}
	payload := make([]byte, MaxRecordSize, MaxRecordSize)
	return Reader{
		f: f,
		fileSize: info.Size(),
		off: 0,
		header: make([]byte, 16, 16),
		buf: payload,
		Record: Record{
			recType: 0,
			size:    0,
			Payload: nil,
		},
	}
}

func (r *Reader)ReadNext() bool {
	if r.fileSize <= r.off {
		return false
	}
	n, err := r.f.ReadAt(r.header, int64(r.off))
	if err != nil {
		panic(err.Error())
	}
	if n != HeadSize {
		panic("...")
	}
	r.Record.recType = int(binary.LittleEndian.Uint64(r.header))
	r.Record.size = int(binary.LittleEndian.Uint64(r.header[8:]))
	r.Record.Payload = r.buf[:r.Record.size]
	n, err = r.f.ReadAt(r.Record.Payload, r.off+int64(HeadSize))
	if err != nil {
		panic(err.Error())
	}
	if n != r.Record.size {
		panic("...")
	}

	r.off += int64(HeadSize + r.Record.size)
	return true
}