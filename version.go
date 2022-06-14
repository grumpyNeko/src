package mydb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mydb/sstable"
	"os"
	"path"
)

const MaxLevelIndex int = 6

type Version struct {
	dbdir    string
	Levels   []*LevelMeta
	GlobalFn int // not used yet
	GlobalSn int // not used yet
}

func RecoverFrom(dbdir string, filename string) *Version {
	versionFile, err := os.OpenFile(path.Join(dbdir, filename),  os.O_RDONLY, 0644)
	if err != nil {
		panic(err.Error())
	}
	var ret Version

	res, err := ioutil.ReadAll(versionFile)
	if err != nil {
		panic(err.Error())
	}
	err = json.Unmarshal(res, &ret)
	if err != nil {
		panic(err.Error())
	}

	for i:=0; i<len(ret.Levels); i++ {
		if ret.Levels[i] == nil {
			ret.Levels[i] = NewLevelMeta()
		}
	}

	openSST := func() {  // todo: 抽出一个函数
		for i:=0; i < len(ret.Levels); i++ {
			level := ret.Levels[i]
			iter := level.Iter()
			if !iter.First() {
				break
			}
			for {
				sst := iter.Get()
				sstFile, err := os.OpenFile(path.Join(dbdir, fmt.Sprintf("%d.sst", sst.Meta.FileNum)),  os.O_RDONLY, 0644)
				if err != nil {
					sst.F = nil
				} else {
					sst.F = sstFile
				}

				if !iter.Next() {
					break
				}
			}
		}
	}
	openSST()

	return &ret
}

func Init(dbdir string) *Version {
	ret :=  Version{
		dbdir: dbdir,
		Levels:   make([]*LevelMeta, MaxLevelIndex+1, MaxLevelIndex+1),
		GlobalFn: 0,
		GlobalSn: 0,
	}
	for i:=0; i<len(ret.Levels); i++ {
		if ret.Levels[i] == nil {
			ret.Levels[i] = NewLevelMeta()
		}
	}
	return &ret
}

func (v *Version)Add(sst *sstable.SST) {
	v.Levels[sst.Meta.Level].Add(*sst)
}

func (v *Version)Del(target *sstable.SST) {
	v.Levels[target.Meta.Level].Del(target.Meta.FileNum)
}

func (v *Version)Save() {
	filename := fmt.Sprintf("%d.meta", v.GlobalFn)
	v.GlobalFn ++
	f, err := os.OpenFile(path.Join(v.dbdir, filename),  os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {panic(err.Error())}
	b, err := json.Marshal(*v)
	if err != nil {
		panic(err.Error())
	}
	f.Write(b)
	f.Sync()
}

func (v *Version)AllocFileNum() int {
	ret := v.GlobalFn
	v.GlobalFn ++
	return ret
}

func (v *Version)AllocSeqNum() int {
	ret := v.GlobalSn
	v.GlobalSn ++
	return ret
}

func (v *Version)DeepCopy() *Version {
	newVersion := &Version{
		Levels:   make([]*LevelMeta, MaxLevelIndex+1, MaxLevelIndex+1),
		GlobalFn: v.GlobalFn,
		GlobalSn: v.GlobalSn,
	}
	for i:=0; i<=MaxLevelIndex; i++ {
		l := v.Levels[i].DeepCopy()
		newVersion.Levels[i] = &l // *newVersion.Levels[i]会解引用nil
	}
	return newVersion
}