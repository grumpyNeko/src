package mydb

import (
	"mydb/base"
	"mydb/logger"
	"mydb/skl"
	"mydb/sstable"
	"path"
	"testing"
	"time"
)

func Test_write_compactCandidates(t *testing.T) {
	dirpath := "../testdata/compact"

	version := Init(dirpath)
	version.GlobalSn = 0x00000000

	ukey := 0
	// 0,0 ~ 339,339
	b := sstable.NewSSTBuilder(dirpath, version.AllocFileNum(), 1)
	for ct:=0; ct<sstable.RecordInDataPage*2; ct++ { // 12-3-1
		ikey := base.InternalKey{
			UserKey: uint64(ukey),
			Tail: uint64(version.AllocSeqNum()),
		}
		ukey ++
		b.Add(ikey, uint64(ct))
	}
	version.Add(b.Done())

	// {0,340} 16
	b = sstable.NewSSTBuilder(dirpath, version.AllocFileNum(), 0)
	ukey = 0
	for ct:=0; ct<1; ct++ {
		ikey := base.InternalKey{
			UserKey: uint64(ukey),
			Tail: uint64(version.AllocSeqNum()),
		}
		ukey ++
		b.Add(ikey, uint64(16))
	}
	version.Add(b.Done())

	// {1,341}, 16
	b = sstable.NewSSTBuilder(dirpath, version.AllocFileNum(), 0)
	ukey = 1
	for ct:=0; ct<1; ct++ {
		ikey := base.InternalKey{
			UserKey: uint64(ukey),
			Tail: uint64(version.AllocSeqNum()),
		}
		ukey ++
		b.Add(ikey, uint64(16))
	}
	version.Add(b.Done())

	// {1024,342} 16
	b = sstable.NewSSTBuilder(dirpath, version.AllocFileNum(), 0)
	ukey = 1024
	for ct:=0; ct<1; ct++ {
		ikey := base.InternalKey{
			UserKey: uint64(ukey),
			Tail: uint64(version.AllocSeqNum()),
		}
		ukey ++
		b.Add(ikey, uint64(16))
	}
	version.Add(b.Done())


	version.Save()
	time.Sleep(1000)
}

// 用于测试db.pointsearch db.rangesearch
func Test_write_commonTest(t *testing.T) {
	dbdir := "./testdata/common"
	version := Init(dbdir)
	//version.GlobalSn = 0x00000000
	// --- --- --- --- --- --- --- --- --- --- --- ---
	b := sstable.NewSSTBuilder(dbdir, 0, 1)
	for i:=1; i<=sstable.RecordInDataPage*(512+1); i++ {
		ikey := base.InternalKey{
			UserKey: uint64(i),
			Tail: uint64(i),
		}
		b.Add(ikey, uint64(i))
	}
	version.Add(b.Done())
	// --- --- --- --- --- --- --- --- --- --- --- ---
	b = sstable.NewSSTBuilder(dbdir, 1, 0)
	b.Add(base.InternalKey{
		UserKey: uint64(370),
		Tail: uint64(100000),
	}, uint64(100000))
	b.Add(base.InternalKey{
		UserKey: uint64(470),
		Tail: uint64(100001),
	}, uint64(100001))
	version.Add(b.Done())
	// --- --- --- --- --- --- --- --- --- --- --- ---
	b = sstable.NewSSTBuilder(dbdir, 2, 0)
	b.Add(base.InternalKey{
		UserKey: uint64(1),
		Tail: uint64(110001),
	}, uint64(110001))
	b.Add(base.InternalKey{
		UserKey: uint64(170),
		Tail: uint64(110000),
	}, uint64(110000))
	version.Add(b.Done())
	// --- --- --- --- --- --- --- --- --- --- --- ---
	b = sstable.NewSSTBuilder(dbdir, 3, 0)
	b.Add(base.InternalKey{
		UserKey: uint64(300),
		Tail: uint64(120000),
	}, uint64(120000))
	b.Add(base.InternalKey{
		UserKey: uint64(301),
		Tail: uint64(120001),
	}, uint64(120001))
	version.Add(b.Done())
	// --- --- --- --- --- --- --- --- --- --- --- ---
	b = sstable.NewSSTBuilder(dbdir, 4, 0)
	b.Add(base.InternalKey{
		UserKey: uint64(200),
		Tail: uint64(130003),
	}, uint64(130003))
	b.Add(base.InternalKey{
		UserKey: uint64(300),
		Tail: uint64(130001),
	}, uint64(130001))
	b.Add(base.InternalKey{
		UserKey: uint64(370),
		Tail: uint64(130000),
	}, uint64(130000))
	version.Add(b.Done())

	// --- --- --- --- --- --- --- --- --- --- --- ---
	mem0 := skl.NewSkiplist()
	mem0.Add(base.InternalKey{
		UserKey: 300,
		Tail:    200001,
	}, 200001)
	mem0.Add(base.InternalKey{
		UserKey: 302,
		Tail:    200000,
	}, 200000)
	mem0.Add(base.InternalKey{
		UserKey: 87210,
		Tail:    200002,
	}, 200002)
	MemtableToInputFile(dbdir, 5, mem0)
	// --- --- --- --- --- --- --- --- --- --- --- ---
	mem1 := skl.NewSkiplist()
	mem1.Add(base.InternalKey{
		UserKey: 300,
		Tail:    210000,
	}, 210000)
	MemtableToInputFile(dbdir, 6, mem1)
	// --- --- --- --- --- --- --- --- --- --- --- ---
	version.GlobalSn = 210001
	version.GlobalFn = 7
	version.Save()
}

func Test_version_edit(t *testing.T) { // todo:
	dbdir := "./testdata/version" // 里面是common里的7.meta

	// 创建9.sst, 它在group.Apply(current)中被打开
	b := sstable.NewSSTBuilder(dbdir, 9, 1)
	b.Add(base.InternalKey{
		UserKey: uint64(370),
		Tail: uint64(100000),
	}, uint64(100000))
	b.Add(base.InternalKey{
		UserKey: uint64(470),
		Tail: uint64(100001),
	}, uint64(100001))
	b.Done()
	// 删除已存在的8.log
	filename := path.Join(dbdir, base.LogFileName(8))
	w := logger.NewWriter(filename)
	ve0 := &versionEdit{
		sn:      210002,
		fn:      10,
		added:   []tablePos{
			{
				fn:    9,
				level: 0,
			},
		},
		deleted:  []tablePos{},
	}
	w.Write(ve0.Encode(), 1)
	ve1 := &versionEdit{
		sn:      210003,
		fn:      10,
		added:   []tablePos{
			{
				fn:    9,
				level: 1,
			},
		},
		deleted:  []tablePos{
			{
				fn:    9,
				level: 0,
			},
		},
	}
	w.Write(ve1.Encode(), 1)

	r := logger.NewReader(filename)
	group := NewVeGroup()
	for r.ReadNext() {
		ve := Decode(r.Record.Payload)
		group.Accumulate(ve)
	}

	current := RecoverFrom(dbdir, base.MetaFileName(7))
	current.dbdir = dbdir
	current = group.Apply(current)
	println("debug this line to check current")
}