package base

type InternalKey struct {
	UserKey uint64
	Tail    uint64
}

//func CreatePointSearchKey(ukey uint64)InternalKey {
//	return InternalKey{
//		UserKey: ukey,
//		Tail:    MaximumUint64,
//	}
//}

func Compare(i1 InternalKey, i2 InternalKey) int {
	if i1.UserKey > i2.UserKey {
		return 1
	} else if i1.UserKey < i2.UserKey {
		return -1
	} else if i1.Tail > i2.Tail {
		return 1
	} else if i1.Tail < i2.Tail {
		return -1
	} else {
		return 0
	}
}