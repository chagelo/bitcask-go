package index

import (
	"path/filepath"

	"go.etcd.io/bbolt"

	"bitcask-go/data"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketname = []byte("bitcask-index")

// B+ 树索引
// 封装了 go.etcd.io/bbolt 库

type BPlusTree struct {
	tree *bbolt.DB
}

// BPlusTree 初始化 B+ 树索引
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	// opts include many customed settings
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}

	// 创建对应的 bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(indexBucketname)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{tree: bptree}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketname)
		oldVal = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}

	if len(oldVal) == 0 {
		return nil
	}

	return data.DecodeLogRecordPos(oldVal)
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketname)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}

	return pos
}

func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketname)
		if oldVal = bucket.Get(key); len(oldVal) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}

	if len(oldVal) == 0 {
		return nil, false
	}

	return data.DecodeLogRecordPos(oldVal), true
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketname)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bptree")
	}

	return size
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

// B+ 树迭代器
type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}

	bpti := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketname).Cursor(),
		reverse: reverse,
	}
	bpti.Rewind()

	return bpti
}

func (bpti *bptreeIterator) Rewind() {
	if bpti.reverse {
		bpti.currKey, bpti.currValue = bpti.cursor.Last()
	} else {
		bpti.currKey, bpti.currValue = bpti.cursor.First()
	}
}

func (bpti *bptreeIterator) Seek(key []byte) {
	bpti.currKey, bpti.currValue = bpti.cursor.Seek(key)
}

func (bpti *bptreeIterator) Next() {
	if bpti.reverse {
		bpti.currKey, bpti.currValue = bpti.cursor.Prev()
	} else {
		bpti.currKey, bpti.currValue = bpti.cursor.Next()
	}
}

func (bpti *bptreeIterator) Valid() bool {
	return len(bpti.currKey) != 0
}

func (bpti *bptreeIterator) Key() []byte {
	return bpti.currKey
}

func (bpti *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpti.currValue)
}

func (bpti *bptreeIterator) Close() {
	_ = bpti.tx.Rollback()
}
