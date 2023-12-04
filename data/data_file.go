package data

import "bitcask-go/fio"

const DataFileNameSuffix = ".data"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        // 文件 id
	WriteOff  uint64         // 文件写偏移
	IoManager fio.IOManager // io 读写管理
}

// OpenDataFile 打开新的数据文件，需要初始化 FileId 和 WriteOff
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) ReadLogRecord(offset uint64) (*LogRecord, uint64, error) {
	return nil, 0, nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
func EncodeLogRecord(LogRecord *LogRecord) ([]byte, uint64) {
	return nil, 0
}
