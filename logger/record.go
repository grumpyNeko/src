package logger

const HeadSize int = 16
const MaxRecordSize int = 1 << 16

type Record struct {
	recType int
	size    int // byte
	Payload []byte
}
