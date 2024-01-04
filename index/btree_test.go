package index

import (
	"bitcask-go/data"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.Equal(t, uint32(1), res3.Fid)
	assert.Equal(t, uint64(2), res3.Offset)
}


func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	pos1  := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, uint64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Equal(t, uint32(1), res3.Fid)
	assert.Equal(t, uint64(2), res3.Offset)


	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, uint64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	res2, ok1 := bt.Delete(nil)
	assert.True(t, ok1)
	assert.Equal(t, uint32(1), res2.Fid)
	assert.Equal(t, uint64(100), res2.Offset)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.Nil(t, res3)
	res4, ok2 := bt.Delete([]byte("aaa"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(22), res4.Fid)
	assert.Equal(t, uint64(33), res4.Offset)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	// 1.BTree 为空的情况
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	//	2.BTree 有数据的情况
	bt1.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3.有多条数据
	bt1.Put([]byte("acee"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("eede"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// 4.测试 seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// 5.反向遍历的 seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}