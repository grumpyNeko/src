package base

type bitSet []bool

func newBitSet(n int) bitSet {
	return make([]bool, n)
}

func (b *bitSet) markBit(i int) {
	(*b)[i] = true
}

func (b *bitSet) markBits(start, end int) {
	for i := start; i < end; i++ {
		(*b)[i] = true
	}
}

func (b *bitSet) clearAllBits() {
	for i := range *b {
		(*b)[i] = false
	}
}