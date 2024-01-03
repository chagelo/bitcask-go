package fio

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/tmp/kv-go", "mmap-a.data")
	defer destroyFile(path)

	mmapIO, err := NewMMapIOManager(path)
	assert.Nil(t, err)
	
	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	assert.Equal(t, 0, n1)
	assert.Equal(t, io.EOF, err)


	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("dsa"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("cvx"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("321"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("329"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)
	size, err := mmapIO2.Size()
	assert.Nil(t, err)
	assert.Equal(t, uint64(12), size)

	b2 := make([]byte, 3)
	n2, err := mmapIO2.Read(b2, 0)
	assert.Nil(t, err)
	assert.Equal(t, 3, n2)
}