package sstable

import (
	"encoding/json"
	"fmt"
	base "mydb/base"
	"os"
	"strings"
)

const (
	PageSize 		 int = 1 << 12 // 4K
	UserKeySize      int = 8
	RecordInDataPage int = 170
	TVOffset         int = UserKeySize * RecordInDataPage
)

type SST struct {
	F    *os.File `json:"-"`
	Meta *SSTMeta `json:"Meta,omitempty"`
}


type SSTMeta struct {
	FileNum         int
	Created   		int64
	Level     		int
	UkeyRange     	*base.Range
	TailRange     	*base.Range
	DataPageCount 	int
	SmallestSublevelIndex int
	LargestSublevelIndex  int
}

//func (m SSTMeta)Pretty() string {
//
//}

// todo: s.F, 应该是只读模式打开, 所以不 deep copy
func (s SST)DeepCopy() SST {
	m := s.Meta.DeepCopy()
	ret := SST{
		F:    s.F,
		Meta: &m,
	}
	return ret
}

func (m SSTMeta)DeepCopy() SSTMeta {
	ukeyr := m.UkeyRange.DeepCopy()
	tailr := m.TailRange.DeepCopy()
	ret := SSTMeta{
		FileNum:       m.FileNum,
		Created:       m.Created,
		Level:         m.Level,
		UkeyRange:     &ukeyr,
		TailRange:     &tailr,
		DataPageCount: m.DataPageCount,
	}
	return ret
}

func (s SST)Pretty() string {
	return s.Meta.Pretty()
}

func (m SSTMeta)Pretty() string {
	return fmt.Sprintf("%d.L%d: {%d}---{%d}\n", m.FileNum, m.Level, m.UkeyRange.Start, m.UkeyRange.End)
}

// for test
type PageDetail []base.Record

// for test
type Detail struct {
	dataPage []PageDetail
	indexes  []uint64
	meta     SSTMeta
}

// for test
func (s SST)GetDetail() Detail {
	dataPageCount := s.Meta.DataPageCount
	ret := Detail{
		dataPage: make([]PageDetail, 0, dataPageCount),
		indexes:  make([]uint64, 0, dataPageCount),
		meta:     *s.Meta,
	}
	indexPageCount := computIndexPageCount(dataPageCount)
	indexPageBuf := make([]byte, indexPageCount*PageSize, indexPageCount*PageSize)
	s.F.ReadAt(indexPageBuf, int64(dataPageCount*PageSize))
	ret.indexes = convertToMemorySearchStructure(indexPageBuf)

	for i:=0; i<dataPageCount; i++ {
		dataPageBuf := make([]byte, PageSize, PageSize)
		s.F.ReadAt(dataPageBuf, int64(i*PageSize))
		iter := NewDataPageIter(dataPageBuf)
		iter.First()
		pageDetail := make([]base.Record, 0, PageSize)
		for {
			pageDetail = append(pageDetail, iter.Get())
			if !iter.Next() {
				break
			}
		}
		ret.dataPage = append(ret.dataPage, pageDetail)
	}

	return ret
}

func readMeta(f *os.File) *SSTMeta {
	info, err := f.Stat()
	if err != nil {
		panic(err.Error())
	}
	size := info.Size()
	if size % int64(PageSize) != 0 {
		panic("sstable file should be K*pagesize large")
	}
	metaPageBuf := make([]byte, PageSize, PageSize)
	n, err := f.ReadAt(metaPageBuf, size - int64(PageSize))
	metaPageBuf = metaPageBuf[:n]
	metaPageBuf = []byte(strings.Trim(string(metaPageBuf), string([]byte{0})))
	if err != nil {
		panic(err.Error())
	}
	var meta SSTMeta
	err = json.Unmarshal(metaPageBuf, &meta)
	if err != nil {
		panic(err.Error())
	}
	return &meta
}

func OpenSSTableFile(sstpath string) *SST {
	f, err := os.OpenFile(sstpath, os.O_RDONLY, 0644)
	if err != nil {
		panic(err.Error())
	}
	return &SST{
		F:    f,
		Meta: readMeta(f),
	}
}