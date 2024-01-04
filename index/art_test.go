package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)
func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	res1 := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res1)
	res2 := art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res2)
	res3 :=art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res3)


	res4 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 99, Offset: 88})
	assert.Equal(t, uint32(1), res4.Fid)
	assert.Equal(t, uint64(12), res4.Offset)
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("key-1"))
	assert.NotNil(t, pos)

	pos1 := art.Get([]byte("not exist"))
	assert.Nil(t, pos1)

	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos2 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos2)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()

	res1, ok1 := art.Delete([]byte("not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	res2, ok2 := art.Delete([]byte("key-1"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(1), res2.Fid)
	assert.Equal(t, uint64(12), res2.Offset)

	pos := art.Get([]byte("key-1"))
	assert.Nil(t, pos)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	assert.Equal(t, 0, art.Size())
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Equal(t, 2, art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("abcd"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("sadsd"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("qweqww"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("zxc123"), &data.LogRecordPos{Fid: 1, Offset: 12})

	iter := art.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t,iter.Key())
		assert.NotNil(t, iter.Value())
	}
}