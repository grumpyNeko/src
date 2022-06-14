package filter

import (
	"math"
	"mydb/base"
	"testing"
)

func Test_filter(t *testing.T) {
	f := NewFilter(math.MaxUint64)
	records := []base.Record{
		base.Record{
			Key: base.InternalKey{
				UserKey: 1,
				Tail:    1,
			},
			Val: 1,
		},
		base.Record{
			Key: base.InternalKey{
				UserKey: 1,
				Tail:    2,
			},
			Val: 12,
		},
		base.Record{
			Key: base.InternalKey{
				UserKey: 2,
				Tail:    10,
			},
			Val: 20,
		},
	}
	for _,r := range records {
		result := f.Feed(r)
		if result != nil {
			println(result.Pretty())
			break
		}
	}
	println(f.Done().Pretty())
}