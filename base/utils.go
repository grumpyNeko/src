package base

import (
	"math"
	"math/rand"
	"time"
)

const MinimumUint64 uint64 = 0
const MaximumUint64 uint64 = math.MaxUint64

func MinUint64(a... uint64) uint64 {
	min := MaximumUint64
	for i:=0; i<len(a); i++ {
		n := a[i]
		if n < min {
			min = n
		}
	}
	return min
}

func MaxUint64(a... uint64) uint64 {
	max := MinimumUint64
	for i:=0; i<len(a); i++ {
		n := a[i]
		if n > max {
			max = n
		}
	}
	return max
}


type Uint64Generator struct {
	lowerbound 		uint64
	upperbound 		uint64
	dense           bool
	noDuplicate     bool
	//filter          map[uint64]bool
	seed            rand.Source
	size            int
	//list            []uint64
}

func  NewUint64Generator(lowerbound uint64, upperbound uint64, dense bool, noDuplicate bool, size int) *Uint64Generator {
	if dense && noDuplicate {
		if int(upperbound) - int(lowerbound) + 1 != size {
			panic("inconsistent parameters")
		}
	}
	return &Uint64Generator{
		lowerbound:  lowerbound,
		upperbound:  upperbound,
		dense:       dense,
		noDuplicate: noDuplicate,
		//filter:      make(map[uint64]bool, size),
		seed:        nil,
		size:        size,
		//list:        make([]uint64, 0, size),
	}
}

func (g *Uint64Generator)Generate() []uint64 {
	g.seed = rand.NewSource(time.Now().UnixNano())
	list := make([]uint64, 0, g.size)
	filter := make(map[uint64]bool, g.size)

	ct := 0
	for ct < g.size {
		randomNum := uint64(rand.Intn(int(g.upperbound)-int(g.lowerbound)+1)) + g.lowerbound
		if _,found := filter[randomNum]; found {
			continue
		}
		list = append(list, randomNum)
		filter[randomNum] = true
		ct++
	}
	return list
}

