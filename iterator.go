package bitcask_go

import (
	"bitcask-go/index"
)

type Iterator struct {
	indexIter index.Iterator // 索引迭代器
	db        *DB
	options   IteratorOptions
}

// NewItertor 初始化迭代器
func (db *DB) NewItertor(opts IteratorOptions) *Iterator{
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator {
		db : db,
		indexIter: indexIter,

	}
}

// Rewind 重新回到迭代器的起点，第一个数据
func (it *Iterator) Rewind(opts IteratorOptions) {
	it.indexIter.Rewind()
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据这个 key 开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
}

// Next 跳转到下一个 key
func (it *Iterator) Next() {
	it.indexIter.Next()
}

// Valid 是否有效
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 当前遍历位置的 key 的数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 当前遍历位置的 value
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValuesByPosition(logRecordPos)
}

// Close 关闭迭代器，释放对应的资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}


func (it *Iterator) skipToNext() {
	
}