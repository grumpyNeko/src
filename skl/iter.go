package skl

import (
	"mydb/base"
)

// *Iter implements the TableIter interface.
var _ base.TableIter = (*Iter)(nil)

type Iter struct {
	list  *Skiplist
	nd    *node
	//key   mysst.InternalKey
	//lower uint64
	//upper uint64
}

func (s *Skiplist) Iter() *Iter {
	return &Iter{
		list: s,
		nd:   s.head,
	}
}

func (it *Iter) SeekGE(ikey base.InternalKey) bool {
	ws := it.list.findWindows(ikey)
	it.nd = ws[minLevel].succ
	if it.nd == it.list.tail {
		return false
	}
	return true
}

func (it *Iter) SeekLT(ikey base.InternalKey) bool {
	ws := it.list.findWindows(ikey)
	it.nd = ws[minLevel].pred
	if it.nd == it.list.head {
		return false
	}
	return true
}

func (it *Iter) First() bool {
	if it.list.Size == 0 {
		return false
	}
	it.nd = it.list.head.next[minLevel]
	return true
}

func (it *Iter) Next() bool {
	it.nd = it.nd.next[minLevel]
	if it.nd != it.list.tail {
		return true
	} else {
		return false
	}
}

func (it *Iter) Get() base.Record {
	return base.Record{
		Key: it.nd.key,
		Val: it.nd.val,
	}
}

func (it *Iter) Close() {
	// do nothing
	// maybe set valid field to false
}