package index

import (
	"bitcask-go/data"
	"sync"
	"github.com/google/btree"
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


func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	/* 
		insert get delete 方法参数是接口，传入非接口类型需要
		该类型实现所有接口中的方法
	*/
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
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

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	// if oldItem == nil {
	// 	return false
	// }
	// return true
	return oldItem != nil
}