package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

// Indexer 索引接口，对于不同数据结构，实现这个接口即可
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	
	// 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// 根据 key 删除对应的索引位置信息
	Delete(key []byte) bool
}

type IndexType = int8

const (
	// BTree 索引
	Btree IndexType = iota + 1
	

	// ATR 自适应基数树索引
	ART
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		// todo
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}