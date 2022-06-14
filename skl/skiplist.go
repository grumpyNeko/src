package skl

import (
	"math"
	base "mydb/base"
	"mydb/skl/fastrand"
)

type Skiplist struct {
	head	*node
	tail *node
	Size int
}

type window struct {
	pred    *node
	succ	*node
}

func NewSkiplist() *Skiplist {
	head := node{
		key: base.InternalKey{
			UserKey: 0,
			Tail:    0,
		},
		val:  0,
		next: [9]*node{},
		meta: 9,
	}

	tail := node{
		key: base.InternalKey{
			UserKey: math.MaxUint64,
			Tail:    math.MaxUint64,
		},
		val:  0,
		next: [9]*node{},
		meta: 9,
	}

	for i:=8; i>=0; i-- {
		head.next[i] = &tail
	}

	return &Skiplist{
		head: &head,
		tail: &tail,
		Size: 0,
	}
}

/*
pred.key < targetKey <= succ.key
 */
func (s *Skiplist) findWindowForLevel(targetKey base.InternalKey, cuLe int, start *node) (pred *node, succ *node) {
	pred = start
	succ = pred.next[cuLe]
	for {
		if succ == s.tail || base.Compare(targetKey, succ.key) <= 0 {
			break
		}
		pred = succ
		succ = pred.next[cuLe]
	}
	return pred, succ
}

func (s *Skiplist) findWindows(targetKey base.InternalKey) (windows []window) {
	pred := s.head
	var succ *node
	windows = make([]window, 9, 9)
	for cuLe := 8; cuLe>=0; cuLe-- {
		pred, succ = s.findWindowForLevel(targetKey, cuLe, pred)
		windows[cuLe].pred = pred
		windows[cuLe].succ = succ
	}

	return windows
}

const (
	minLevel    = 0
	maxLevel    = 8
	maxHeight   = 9
	pValue      = 1 / math.E
)

var probabilities [9]uint32 // probabilities[0]是math.MaxUint32, 无用

func init() {
	p := 1.0
	for i := 0; i < maxHeight; i++ {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}

func (s *Skiplist)generateNewHeight() int {
	rnd := fastrand.Uint32()

	h := 1
	for h < maxHeight && rnd <= probabilities[h] {
		h++
	}

	return h
}

func (s *Skiplist)Add(key base.InternalKey, vp uint64) {
	windows := s.findWindows(key)

	h := s.generateNewHeight()
	nd := node{
		key:  key,
		val:  vp,
		next: [9]*node{},
		meta: uint32(h),
	}

	for i := 0; i <= int(h-1); i++ {
		pred, succ := windows[i].pred, windows[i].succ
		nd.next[i] = succ
		pred.next[i] = &nd
	}
	s.Size++
}

// todo: 这个方法可以被iter.SeekLe()代替
// todo: 标记删除会影响这里的逻辑
func (s *Skiplist)Get(key base.InternalKey) (*base.InternalKey, *uint64) {
	ws := s.findWindows(key)
	hopeful := ws[0].succ // todo: 如果不等于succ, 应该返回pred
	if hopeful.key.UserKey == key.UserKey { // hopeful.key.Tail <= key.Tail 没必要写
		return &hopeful.key, &hopeful.val
	} else {
		return nil, nil
	}
}

