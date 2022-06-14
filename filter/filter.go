package filter

import "mydb/base"

type filter struct {
	r 	*base.Record
	sn  uint64
}

func NewFilter(sn uint64) *filter {
	return &filter{
		r:  nil,
		sn: sn, // todo: delete, snapshot
	}
}

func (f *filter)Feed(record base.Record) (poop *base.Record) {
	if f.r == nil {
		f.r = &record
		return nil
	}

	if f.r.Key.UserKey != record.Key.UserKey {
		poop = f.r
	} else {
		poop = nil
		if f.r.Key.Tail >= record.Key.Tail {
			panic("should feed filter in increasing order")
		}
	}
	f.r = &record
	return poop
}

// todo: invalid
func (f *filter)Done() (poop *base.Record) {
	if f.r == nil {
		return nil
	}

	return f.r
}


















