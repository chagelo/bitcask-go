package index

import (
	"bytes"

	"github.com/google/btree"

	"bitcask-go/data"
)

// Indexer 索引接口，对于不同数据结构，实现这个接口即可
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// 根据 key 删除对应的索引位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)

	// 索引迭代器
	Iterator(reverse bool) Iterator

	// 索引中的数据量
	Size() int

	// 关闭索引
	Close() error
}

type IndexType = int8

const (
	// BTree 索引
	Btree IndexType = iota + 1

	// ATR 自适应基数树索引
	ART

	// BPTree B+ 树索引
	BPTree
)

func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return nil
		// return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

// BTree 节点存储的内容
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点，第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效
	Valid() bool

	// Key 当前遍历位置的 key 的数据
	Key() []byte

	// Value 当前遍历位置的 value
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放对应的资源
	Close()
}
