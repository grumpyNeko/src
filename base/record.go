package base

import (
	"fmt"
)

type Record struct {
	Key InternalKey
	Val uint64
}

func (r Record)Pretty() string {
	return fmt.Sprintf("{%-7d, %-7d}:%7d", r.Key.UserKey, r.Key.Tail, r.Val)
}