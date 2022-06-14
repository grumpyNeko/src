package sstable

import (
	"encoding/binary"
	"mydb/base"
)

type DataPageIter struct {
	buf						[]byte
	totalRecordCount		int
	currnetIndex  			int
	currentRecord 			base.Record
	nextRecord    			base.Record

	valid         			bool
}


func NewDataPageIter(buf []byte) *DataPageIter {
	return &DataPageIter{
		buf:              buf,
		totalRecordCount: int(binary.LittleEndian.Uint16(buf[PageSize-2:])),
		currnetIndex:     0,
		currentRecord:    base.Record{},
		nextRecord:       base.Record{},
		valid:            false,
	}
}

func (iter *DataPageIter)First()bool {
	if iter.totalRecordCount == 0 {
		return false
	}
	iter.currnetIndex = 0
	iter.currentRecord = parseRecord(iter.buf, iter.currnetIndex)
	iter.nextRecord = parseRecord(iter.buf, iter.currnetIndex+1)

	iter.valid = true
	return true
}

// SeekLe
/*
iter.valid = true
// 遍历, 一定在这里面
for j in [0, totalRecordCount-1)
  currnetIndex = j
  currentRecord, nextRecord <- buf, currnetIndex
  if nextRecord.Key > target.UserKey
    return true

return false // 理论上到不了这里
*/
func (iter *DataPageIter)SeekLe(target base.InternalKey) bool {
	iter.valid = true


	if base.Compare(parseRecord(iter.buf, 0).Key, target) > 0 { // 万一第一个就比target大
		return false
	}
	for j:=0; j<int(iter.totalRecordCount)-1; j++ {
		// 同时访问当前位置和下一个位置, -1保证总是有nextRecord
		iter.currentRecord = parseRecord(iter.buf, j)
		iter.nextRecord = parseRecord(iter.buf, j+1)

		// 如果下一个key比target大, 定位到当前key
		if base.Compare(iter.nextRecord.Key, target) > 0 {
			iter.currnetIndex = j
			return true
		}
	}

	// 全都比target小, 定位到最后一个
	iter.currnetIndex = int(iter.totalRecordCount - 1)
	iter.currentRecord = parseRecord(iter.buf, iter.currnetIndex)
	return true
}

func parseRecord(buf []byte, index int) (record base.Record) {
	ukeyOff := index* UserKeySize
	record.Key.UserKey = binary.LittleEndian.Uint64(buf[ukeyOff:ukeyOff+8])
	tvOff := TVOffset + index*UserKeySize*2
	record.Key.Tail, record.Val = getTV(buf, tvOff)

	return record
}

func (iter *DataPageIter)SeekGe(target base.InternalKey) bool {

	// 遍历, 一定在这里面
	for j:=0; j<iter.totalRecordCount; j++ {
		// 同时访问当前位置和下一个位置, -1保证总是有nextRecord
		iter.currentRecord = parseRecord(iter.buf, j)

		// if current >= target
		if base.Compare(iter.currentRecord.Key, target) >= 0  {
			iter.currnetIndex = j
			if iter.currnetIndex != iter.totalRecordCount-1 {
				iter.nextRecord = parseRecord(iter.buf, iter.currnetIndex+1)
			}
			iter.valid = true
			return true
		}
	}

	// data-page内所有的都比target小
	// 如果先查index-page就不会有这种情况
	return false
}

/*
if !valid
  panic
if currentIndex > totalRecordCount-1
  return false

currentRecord = nextRecord
if currentIndex+1 <= totalRecordCount-1
  nextRecord <= buf, currentIndex+1
currentIndex ++
return true
 */
func (iter *DataPageIter)Next() bool {
	if !iter.valid {
		panic("iter is not valid!")
	}
	if iter.currnetIndex+1 > iter.totalRecordCount-1 {
		return false
	}

	iter.currentRecord = iter.nextRecord
	iter.currnetIndex ++
	if iter.currnetIndex+1 <= iter.totalRecordCount-1 {
		iter.nextRecord = parseRecord(iter.buf, iter.currnetIndex+1)
	}

	return true
}

func (iter *DataPageIter)Get() base.Record {
/*
Get()
  if !valid
    panic
  return currentRecord
 */
	if !iter.valid {
		panic("iter is not valid!")
	}
	return iter.currentRecord
}

func getTV(page []byte, off int)(tail uint64, val uint64) {
	if off + 16 >= len(page) {
		panic("getTV")
	}
	tail = binary.LittleEndian.Uint64(page[off : off+8])
	val = binary.LittleEndian.Uint64(page[off+8 : off+16])
	return tail, val
}

func (iter *DataPageIter)Close() {
		iter.buf = nil
		iter.totalRecordCount = -1
		iter.currnetIndex = -1
		iter.currentRecord = base.Record{}
		iter.nextRecord = base.Record{}
		iter.valid = false
}