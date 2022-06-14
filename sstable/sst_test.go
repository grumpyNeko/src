package sstable

import (
	"mydb/base"
	"path"
	"testing"
)

func Test_write_commonTest(t *testing.T) {
	dbdir := "../testdata/common"
	sst := OpenSSTableFile(path.Join(dbdir, base.SSTableFileName(1)))
	detail := sst.GetDetail()
	println(detail.meta.FileNum)
}