package data

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// |--crc--|--type--|--keysize--|--valuesize--|--key--|--val--|
// | 4 | 1 | 5 | 5 | - | - |
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

/**
 * LogRecord 写入到数据文件的记录 之所以叫日志
 * 是因为数据文件中的数据是追加写入的，类似日志的格式
 */
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// 头部信息
type logRecordHeader struct {
	crc        uint32        // crc 值
	recordType LogRecordType // LogRecord 类型
	keySize    uint32        // key 长度
	valueSize  uint32        // value 长度
}

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32
	Offset uint64
	Size   uint64 // 标识数据在磁盘上的大小
}

// TransactionRecord 暂存事务相关的数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
// | crc | type | keySize | valSize| key | val |
// | 4 | 1 | 变长（最大5）| 变长（最大5）| 变长 | 变长 |
func EncodeLogRecord(logRecord *LogRecord) ([]byte, uint64) {
	// 初始化一个 header
	header := make([]byte, maxLogRecordHeaderSize)

	// 第五个字节开始写
	header[4] = logRecord.Type
	var index = 5

	index += binary.PutUvarint(header[index:], uint64(len(logRecord.Key)))
	index += binary.PutUvarint(header[index:], uint64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// crc 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	// debug crc
	fmt.Printf("header length: %d, crc: %d\n", index, crc)

	return encBytes, uint64(size)
}

/*
 | pos.Fid | pos.Offset| pos.Size |
 | 变长（4） | 变长（8） | 变长（8） |
*/
// 对位置索引进行编码，LogRecordPos
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64*2)
	var index = 0
	index += binary.PutUvarint(buf[index:], uint64(pos.Fid))
	index += binary.PutUvarint(buf[index:], pos.Offset)
	index += binary.PutUvarint(buf[index:], pos.Size)
	return buf[:index]
}

// 解码位置索引信息
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Uvarint(buf[index:])
	index += n
	offset, n := binary.Uvarint(buf[index:])
	index += n
	size, _ := binary.Uvarint(buf[index:])
	return &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: offset,
		Size: size,
	}
}

// 对字节数组中的 Header 信息进行解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, uint32) {
	// len(buf) <= 4? or <= 5
	if len(buf) <= 5 {
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	// 分别取出 key 和 value
	var index = 5
	keySize, n := binary.Uvarint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Uvarint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, uint32(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
