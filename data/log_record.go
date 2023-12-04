package data


type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

/**
 * LogRecord 写入到数据文件的记录 之所以叫日志
 * 是因为数据文件中的数据是追加写入的，类似日志的格式
*/ 
type LogRecord struct {
	Key []byte
	Value []byte
	Type LogRecordType
}


// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid uint32
	Offset uint64
}