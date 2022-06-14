package skl

import (
	base "mydb/base"
)

type node struct {
	key base.InternalKey
	val uint64
	next [9]*node
	meta uint32
}

