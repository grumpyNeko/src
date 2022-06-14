package base

type TableIter interface {
	//SeekLT(ikey InternalKey) bool
	SeekGE(ikey InternalKey) bool
	Next() bool
	Get() Record
	First() bool
	Close()
}