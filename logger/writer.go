package logger

import (
	"encoding/binary"
	"os"
)

type Writer struct {
	f   *os.File
}

func NewWriter(filepath string) Writer {
	f, err := os.OpenFile(filepath,  os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err.Error())
	}
	return Writer{f: f}
}

func (w *Writer) Write(payload []byte, recType int) {
	header := make([]byte, 16, 16)
	binary.LittleEndian.PutUint64(header, uint64(recType))
	binary.LittleEndian.PutUint64(header[8:], uint64(len(payload)))
	w.f.Write(header)
	w.f.Write(payload)
	w.f.Sync()
}
