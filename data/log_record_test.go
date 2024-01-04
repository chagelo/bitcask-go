package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常情况
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}

	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, uint64(5))
	t.Log(res1, n1)

	// value 为空
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}

	res2, n2 := EncodeLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, uint64(5))
	t.Log(res2, n2)

	// 对 deleted 情况的测试
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}

	res3, n3 := EncodeLogRecord(rec3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, uint64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	headerBuf1 := []byte{154, 6, 195, 152, 0, 4, 10}
	h1, size1 := decodeLogRecordHeader(headerBuf1)

	assert.NotNil(t, h1)
	assert.Equal(t, uint32(7), size1)
	assert.Equal(t, uint32(2562918042), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)

	headerBuf2 := []byte{114, 60, 154, 121, 0, 4, 0}
	h2, size2 := decodeLogRecordHeader(headerBuf2)

	assert.NotNil(t, h2)
	assert.Equal(t, uint32(7), size2)
	assert.Equal(t, uint32(2040151154), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	headerBuf3 := []byte{217, 205, 101, 31, 1, 4, 10}
	h3, size3 := decodeLogRecordHeader(headerBuf3)

	assert.NotNil(t, h3)
	assert.Equal(t, uint32(7), size3)
	assert.Equal(t, uint32(526765529), h3.crc)
	assert.Equal(t, LogRecordDeleted, h3.recordType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}

	headerBuf1 := []byte{154, 6, 195, 152, 0, 4, 10}
	crc1 := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(2562918042), crc1)

	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}

	headerBuf2 := []byte{114, 60, 154, 121, 0, 4, 0}
	crc2 := getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(2040151154), crc2)

	res3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{217, 205, 101, 31, 1, 4, 10}
	crc3 := getLogRecordCRC(res3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(526765529), crc3)
}
