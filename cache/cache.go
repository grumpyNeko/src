package cache

/*
Entry
  key // fileNum, pageNum
  val // page
  prev, next
  ref

Cache
  head, tail, occupied, cap
  keyToEntry

CacheKey
  fileNum
  pageNum
 */

type key struct {
	fileNum int
	pageNum int
}

type Entry struct {
	key 	key
	val 	[]byte
	prev 	*Entry
	next 	*Entry
	ref 	int
	hot     int
}

type FIFOCache struct {
	current  *Entry
	occupied int
	cap      int

	keyToEntry 	map[key]*Entry
}

func NewFIFOCache(cap int) *FIFOCache {
	if cap <= 2 {
		panic("cap too small")
	}

	head := &Entry{
		key:  key{},
		val:  make([]byte, 4096, 4096),
		prev: nil,
		next: nil,
		ref:  0,
		hot:  0,
	}
	head.prev = head
	head.next = head
	for i:=0; i<cap-1; i++ {
		newEntry := &Entry{
			key:  key{},
			val:  make([]byte, 4096, 4096),
			prev: nil,
			next: nil,
			ref:  0,
			hot:  0,
		}
		prev := head
		succ := head.next
		newEntry.prev = prev
		newEntry.next = succ
		prev.next = newEntry
		succ.prev = newEntry
	}

	return &FIFOCache{
		current:    head,
		occupied:   0,
		cap:        cap,
		keyToEntry: make(map[key]*Entry, cap),
	}
}

func (c *FIFOCache)AskFor(fileNum int, pageNum int) (page []byte, found bool) {
	key := key{
		fileNum: fileNum,
		pageNum: pageNum,
	}
	entry, ok := c.keyToEntry[key]
	if ok && entry.key.fileNum == fileNum && entry.key.pageNum == pageNum {
		if entry.ref == 0 {
			c.occupied++
		}
		entry.ref++
		entry.hot++
		return entry.val, true
	}

	// 下面处理未命中的情况
	if c.occupied == c.cap {
		panic("no more space")
	}
	for !(c.current.ref == 0 && c.current.hot == 0) {
		if c.current.hot > 0 {
			c.current.hot--
		}
		c.current = c.current.next
	}
	// victim.ref is -1
	//victim := c.current
	//prev := victim.prev
	//succ := victim.next
	//prev.next = succ
	//succ.prev = prev
	//delete(c.keyToEntry, victim.key)
	//
	////entry = &Entry {
	////	key:  key,
	////	val:  make([]byte, 4096, 4096),	// todo:
	////	prev: nil,
	////	next: nil,
	////	ref:  0,
	//
	//// 复用victim
	//entry = victim
	//entry.key = key
	//entry.ref = 1
	//// 插入到current之前
	//if c.current == nil {
	//	c.current = entry
	//	c.current.next = c.current
	//	c.current.prev = c.current
	//} else {
	//	prev := c.current.prev
	//	prev.next = entry
	//	entry.prev = prev
	//	entry.next = c.current
	//	c.current.prev = entry
	//}

	entry = c.current
	entry.key = key
	delete(c.keyToEntry, entry.key)
	c.keyToEntry[key] = entry
	entry.ref = 1
	entry.hot = 1

	c.current = c.current.next
	c.occupied++

	return entry.val, false
}



/*
entry.ref = 0
c.occupied--
 */
func (c *FIFOCache)Free(fileNum int, pageNum int) {
	key := key{
		fileNum: fileNum,
		pageNum: pageNum,
	}
	entry, ok := c.keyToEntry[key]
	if ok && entry.key.fileNum == fileNum && entry.key.pageNum == pageNum {
		entry.ref = 0
		c.occupied--
	} else {
		panic("already freed!")
	}
}