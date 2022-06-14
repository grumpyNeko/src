package cache

import (
	"encoding/binary"
	"testing"
)

func Test_hash(t *testing.T) {
	keyToEntry := make(map[key]*Entry, 1024)
	key0 := key{
		fileNum: 1,
		pageNum: 0,
	}
	key1 := key{
		fileNum: 1,
		pageNum: 0,
	}
	e := &Entry{
		key:  key0,
		val:  nil,
		prev: nil,
		next: nil,
		ref:  0,
	}

	keyToEntry[key0] = e
	_, ok := keyToEntry[key1]
	if ok {
		println("ok")
	} else {
		println("not found")
	}
}

// only for test purpose
func (c *FIFOCache)getWithoutTrace(fileNum int, pageNum int) (page []byte, found bool) {
	key := key{
		fileNum: fileNum,
		pageNum: pageNum,
	}
	entry, ok := c.keyToEntry[key]
	if ok && entry.key.fileNum == fileNum && entry.key.pageNum == pageNum {
		return entry.val, true
	} else {
		return nil, false
	}
}

func Test_cache(t *testing.T) {
	check := func(cache *FIFOCache, filenum int, pagenum int) {
		buf, found := cache.getWithoutTrace(filenum, pagenum)
		if found {
			println(binary.LittleEndian.Uint16(buf))
			cache.Free(filenum, pagenum)
		} else {
			println("not found")
		}
	}
	add := func(cache *FIFOCache, filenum int, pagenum int, val int) {
		buf, _ := cache.AskFor(filenum, pagenum)
		binary.LittleEndian.PutUint16(buf, uint16(val))
	}
	cache := NewFIFOCache(3)
	add(cache, 1, 0, 10)
	add(cache, 1, 1, 11)
	add(cache, 1, 2, 12)

	check(cache, 1, 0)
	check(cache, 1, 1)
	check(cache, 1, 2)
	check(cache, 2, 0) // not found

	cache.Free(1,0)
	cache.Free(1,1)
	cache.Free(1,2)
	println("--- --- --- ---")
	add(cache, 1, 0, 10-5)
	add(cache, 1, 2, 12-5)
	add(cache, 2, 0, 20-5)

	check(cache, 1, 0) // hot 0
	check(cache, 1, 1) // not found
	check(cache, 1, 2) // hot 1  <-- current
	check(cache, 2, 0) // hot 1
/*
 0   1    1
10 - 20 - 12
 */
	cache.Free(1,0)
	cache.Free(1,2)
	cache.Free(2,0)
	println("--- --- --- ---")
}