package base

type Range struct {
	Start uint64
	End   uint64
}

func (r Range)Contain(r2 Range) bool {
	if r.Start < r2.Start && r.End > r2.End {
		return true
	} else {
		return false
	}
}

func (r Range)Union(r2 Range) (ret Range) {
	if !r.HasOverlap(r2) { // todo:
		panic("only support overlap range")
	}

	ret.Start = MinUint64(r.Start, r2.Start)
	ret.End = MaxUint64(r.End, r2.End)

	return ret
}

//func (r Range)Intersection(r2 Range) (ret Range) {
//	ret.Start = MinUint64(r.Start, r2.Start)
//	ret.End = MaxUint64(r.End, r2.End)
//	return ret
//}

//func (r Range)Subtraction(r2 Range) (ret Range) {
//
//}

func (r Range)DeepCopy() Range {
	ret := Range{
		Start: r.Start,
		End:   r.End,
	}
	return ret
}

func (r Range)HasOverlap(r2 Range) bool {
	if r.Start > r2.End || r.End < r2.Start {
		return false
	} else {
		return true
	}
}
