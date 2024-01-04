package index

import (
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"

	"bitcask-go/data"
)

// BTree 索引，封装了 google 的 btree 库
type BTree struct {
	tree *btree.BTree
	// google btree 的实现对于多线程写操作不是安全的
	lock *sync.RWMutex
}

// NewBTree 初始化 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	/*
		insert get delete 方法参数是接口，传入非接口类型需要
		该类型实现所有接口中的方法
	*/
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()

	if oldItem == nil {
		return nil
	}

	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	bt.lock.Lock()
	btreeItem := bt.tree.Get(it)
	bt.lock.Unlock()
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}

	return oldItem.(*Item).pos, true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Close() error {
	return nil
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// BTree 索引迭代器
type btreeIterator struct {
	currIndex int     // 当前遍历的下标位置
	reverse   bool    // 是否反向遍历
	values    []*Item // key+位置索引信息
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	// 潜在问题，可能导致内存突然膨胀
	values := make([]*Item, tree.Len())

	saveVaules := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveVaules)
	} else {
		tree.Ascend(saveVaules)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}

}

// Rewind 重新回到迭代器的起点，第一个数据
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据这个 key 开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}

}

// Next 跳转到下一个 key
func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

// Valid 是否有效
func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

// Key 当前遍历位置的 key 的数据
func (bti *btreeIterator) Key() []byte {
	if !bti.Valid() {
		panic("iterator out of bound")
	}
	return bti.values[bti.currIndex].key
}

// Value 当前遍历位置的 value
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

// Close 关闭迭代器，释放对应的资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}
