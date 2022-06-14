package mydb

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"mydb/base"
	"mydb/skl"
)



func MemtableToInputFile(dir string, fn int, skl *skl.Skiplist) {
	filename := fmt.Sprintf("%d.input", fn)
	f, err := os.OpenFile(path.Join(dir, filename), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err.Error())
	}
	defer func() {
		f.Sync()
		f.Close()
	}()

	buf := make([]byte, 24*skl.Size, 24*skl.Size)
	byteOffset := 0
	it := skl.Iter()
	if !it.First() {
		return
	}
	for {
		r := it.Get()
		binary.LittleEndian.PutUint64(buf[byteOffset:], r.Key.UserKey)
		byteOffset += 8
		binary.LittleEndian.PutUint64(buf[byteOffset:], r.Key.Tail)
		byteOffset += 8
		binary.LittleEndian.PutUint64(buf[byteOffset:], r.Val)
		byteOffset += 8

		if !it.Next() {
			break
		}
	}

	f.Write(buf)
}

func InputFileToMemtable(dir string, fn int) *skl.Skiplist {
	list := skl.NewSkiplist()
	filename := fmt.Sprintf("%d.input", fn)
	buf, _ := ioutil.ReadFile(path.Join(dir, filename))
	iter := NewInputIter(buf)
	for {
		r := iter.Get()
		list.Add(r.Key, r.Val)
		if !iter.Next() {
			break
		}
	}
	return list
}

type inputIter struct {
	buf  []byte
	ct   int
}

func NewInputIter(buf []byte) *inputIter {
	return &inputIter{
		buf: buf,
		ct:  0,
	}
}

func (it *inputIter)Next() bool {
	size := len(it.buf) / 24
	if it.ct == size-1 {
		return false
	} else {
		it.ct++
		return true
	}
}

func (it *inputIter)Get() (r base.Record) {
	current := it.buf[it.ct*24:(it.ct+1)*24]

	r.Key.UserKey = binary.LittleEndian.Uint64(current[:8])
	r.Key.Tail = binary.LittleEndian.Uint64(current[8:16])
	r.Val = binary.LittleEndian.Uint64(current[16:24])

	return r
}