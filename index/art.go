package index

import (
	"bytes"
	"sort"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree"

	"bitcask-go/data"
)

// AdapativeRadixTree 自适应基数树索引
type AdapativeRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdapativeRadixTree {
	return &AdapativeRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdapativeRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldVal, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldVal == nil {
		return nil
	}
	return oldVal.(*data.LogRecordPos)
}

func (art *AdapativeRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}

	return value.(*data.LogRecordPos)
}

func (art *AdapativeRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldVal, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldVal == nil {
		return nil, false
	}
	return oldVal.(*data.LogRecordPos), deleted
}

func (art *AdapativeRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

func (art *AdapativeRadixTree) Close() error {
	return nil
}

func (art *AdapativeRadixTree) Iterator(reverse bool) Iterator {
	art.lock.Lock()
	defer art.lock.Unlock()
	return newARTIterator(art.tree, reverse)
}

// BTree 索引迭代器
type artIterator struct {
	currIndex int     // 当前遍历的下标位置
	reverse   bool    // 是否反向遍历
	values    []*Item // key+位置索引信息
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	// 潜在问题，可能导致内存突然膨胀
	values := make([]*Item, tree.Size())
	saveVaules := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveVaules)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}

}

// Rewind 重新回到迭代器的起点，第一个数据
func (arti *artIterator) Rewind() {
	arti.currIndex = 0
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据这个 key 开始遍历
func (arti *artIterator) Seek(key []byte) {
	if arti.reverse {
		arti.currIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) <= 0
		})
	} else {
		arti.currIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) >= 0
		})
	}

}

// Next 跳转到下一个 key
func (arti *artIterator) Next() {
	arti.currIndex += 1
}

// Valid 是否有效
func (arti *artIterator) Valid() bool {
	return arti.currIndex < len(arti.values)
}

// Key 当前遍历位置的 key 的数据
func (arti *artIterator) Key() []byte {
	if !arti.Valid() {
		panic("iterator out of bound")
	}
	return arti.values[arti.currIndex].key
}

// Value 当前遍历位置的 value
func (arti *artIterator) Value() *data.LogRecordPos {
	return arti.values[arti.currIndex].pos
}

// Close 关闭迭代器，释放对应的资源
func (arti *artIterator) Close() {
	arti.values = nil
}
