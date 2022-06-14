package skl

import (
	"math"
	"math/rand"
	base "mydb/base"
	"testing"
	"time"
)

func buildFromUint64(a... uint64) *Skiplist {
	skl := NewSkiplist()
	for i, n := range a {
		skl.Add(base.InternalKey{
			UserKey: n,
			Tail: uint64(i),
		}, n)
	}

	return skl
}

func buildFromInternalKey(a...base.InternalKey) *Skiplist {
	skl := NewSkiplist()
	for i, n := range a {
		skl.Add(n, uint64(i))
	}

	return skl
}

func generateRandomSeq(sn uint64, upperbound uint64, size int) []base.InternalKey {
	if size > int(upperbound) - 1 {
		panic("...")
	}
	rand.Seed(time.Now().Unix())
	ct := 0
	ret := make([]base.InternalKey, size, size)
	filter := make(map[uint64]bool)
	for ct < size {
		randomNum := uint64(rand.Intn(int(upperbound-1)) + 1) // 不包括0
		if _,found := filter[randomNum]; found {
			continue
		}
		ret[ct] = base.InternalKey{
			UserKey: randomNum,
			Tail:    sn + uint64(ct),
		}
		filter[randomNum] = true
		ct++
	}
	return ret
}

func Test_findWindows(t *testing.T) {
	//skl := buildFromUint64(1, 3, 5, 9)
	g := base.NewUint64Generator(1, 1000, true, true, 1000)
	skl := buildFromUint64(g.Generate()...)

	ws := skl.findWindows(base.InternalKey{
		UserKey: uint64(5),
		Tail: math.MaxUint64,
	})

	println(ws[0].pred.key.UserKey)
	println(ws[0].succ.key.UserKey)
}

func Test_iter(t *testing.T) {
	g := base.NewUint64Generator(1, 1000, true, true, 1000)
	skl := buildFromUint64(g.Generate()...)
	iter := Iter{
		list: skl,
		nd:   skl.head,
	}
	//for iter.Next() {
	//	println(iter.nd.key.UserKey)
	//}

	iter.SeekLT(base.InternalKey{
		UserKey: 995,
		Tail:    math.MaxUint64,
	})

	for {
		println(iter.nd.key.UserKey)
		if !iter.Next() {
			break
		}
	}
}
