package mydb

import (
	"encoding/binary"
	"mydb/base"
	"mydb/sstable"
	"path"
)

type tablePos struct {
	fn    int
	level int
}

type versionEdit struct {
	sn int
	fn int
	added   []tablePos
	deleted []tablePos
}

func Decode(buf []byte) versionEdit {
	sn := int(binary.LittleEndian.Uint64(buf))
	buf = buf[8:]
	fn := int(binary.LittleEndian.Uint64(buf))
	buf = buf[8:]
	added := decodeSliceOfTablePos(buf)
	buf = buf[8+16*len(added):]
	deleted := decodeSliceOfTablePos(buf)
	buf = buf[8+16*len(deleted):]
	return versionEdit{
		sn:      sn,
		fn:      fn,
		added:   added,
		deleted: deleted,
	}
}

func (ve versionEdit)Encode() []byte {
	spaceRequired := []int{8+16*len(ve.added), 8+16*len(ve.deleted)}
	overallRequired := 16 + spaceRequired[0] + spaceRequired[1]
	ret := make([]byte, overallRequired, overallRequired)

	buf := ret[:]
	binary.LittleEndian.PutUint64(buf, uint64(ve.sn))
	buf = buf[8:]
	binary.LittleEndian.PutUint64(buf, uint64(ve.fn))
	buf = buf[8:]
	encodeSliceOfTablePos(ve.added, buf)
	buf = buf[spaceRequired[0]:]
	encodeSliceOfTablePos(ve.deleted, buf)
	buf = buf[spaceRequired[1]:]

	return ret
}

func decodeSliceOfTablePos(buf []byte) []tablePos {
	size := int(binary.LittleEndian.Uint64(buf))
	buf = buf[8:]
	ret := make([]tablePos, size, size)
	for i:=0; i<size; i++ {
		ret[i].fn = int(binary.LittleEndian.Uint64(buf))
		buf = buf[8:]
		ret[i].level = int(binary.LittleEndian.Uint64(buf))
		buf = buf[8:]
	}
	return ret
}

func encodeSliceOfTablePos(tablePosList []tablePos, buf []byte) {
	size := len(tablePosList)
	binary.LittleEndian.PutUint64(buf, uint64(size))
	buf = buf[8:]
	for i:=0; i<size; i++ {
		binary.LittleEndian.PutUint64(buf, uint64(tablePosList[i].fn))
		buf = buf[8:]
		binary.LittleEndian.PutUint64(buf, uint64(tablePosList[i].level))
		buf = buf[8:]
	}
}

type veGroup struct { // 目前跟versionEdit一样
	sn int
	fn int
	added   [MaxLevelIndex+1][]int
	deleted [MaxLevelIndex+1][]int
}

func NewVeGroup() *veGroup {
	added := [MaxLevelIndex+1][]int{}
	for i:=0; i<MaxLevelIndex+1; i++ {
		added[i] = make([]int, 0, 8)
	}

	deleted := [MaxLevelIndex+1][]int{}
	for i:=0; i<MaxLevelIndex+1; i++ {
		deleted[i] = make([]int, 0, 8)
	}
	return &veGroup{
		sn:      0,
		fn:      0,
		added:   added,
		deleted: deleted,
	}
}

func (g *veGroup) Accumulate(ve versionEdit) {
	for _,tp := range ve.deleted {
		//deleteInLevel := g.deleted[tp.level] // 此时slice被复制了
		g.deleted[tp.level] = append(g.deleted[tp.level], tp.fn)
	}
	for _,tp := range ve.added {
		//addedInLevel := g.added[tp.level]
		g.added[tp.level] = append(g.added[tp.level], tp.fn)
	}
	g.sn = ve.sn
	g.fn = ve.fn
}

func (g *veGroup) Apply(v *Version) *Version {
	newVersion := v.DeepCopy()
	newVersion.GlobalFn = g.fn
	newVersion.GlobalSn = g.sn
	dbdir := v.dbdir
	for i,l := range g.added {
		target := newVersion.Levels[i]
		for _,fn := range l {
			sst := sstable.OpenSSTableFile(path.Join(dbdir, base.SSTableFileName(fn)))
			target.Add(*sst)
		}
	}
	for i,l := range g.deleted {
		target := newVersion.Levels[i]
		for _,fn := range l {
			target.Del(fn)
		}
	}
	return newVersion
}