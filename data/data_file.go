package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
)

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        // 文件 id
	WriteOff  uint64        // 文件写偏移
	IoManager fio.IOManager // io 读写管理
}

// OpenDataFile 打开新的数据文件，需要初始化 FileId 和 WriteOff
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId)
}

// OpenHintFile 打开 Hint 索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)
}

func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func newDataFile(fileName string, fileId uint32) (*DataFile, error) {
	// 初始化 IOManager
	ioManager, err := fio.NewIOManager(fileName)

	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord 根据 offset 指定的位置读取 LogRecord
func (df *DataFile) ReadLogRecord(offset uint64) (*LogRecord, uint64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 如果读取的最大 header 长度已经超过了文件的长度，则只需要读取到文件的末尾即可
	var headerBytes uint64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > uint64(fileSize) {
		headerBytes = uint64(fileSize) - offset
	}

	// 读取 header
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)
	// 下面两种情况是读到了文件末尾
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	var recordSize = headerSize + header.keySize + header.valueSize

	logRecord := &LogRecord{Type: header.recordType}

	// 开始读取用户实际存储的 key/value 数据
	if header.keySize > 0 || header.valueSize > 0 {
		kvBuf, err := df.readNBytes(uint64(header.keySize)+uint64(header.valueSize), offset+uint64(headerSize))
		if err != nil {
			return nil, 0, err
		}

		if len(kvBuf) <= 0 {
			panic(err)
		}

		// 解出 key 和 value
		logRecord.Key = kvBuf[:header.keySize]
		logRecord.Value = kvBuf[header.keySize:]
	}

	// 校验数据的有效性
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, uint64(recordSize), nil
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += uint64(n)
	return nil
}

// WriteHintRecord 写入索引信息到 hint 文件中
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord {
		Key: key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// read n bytes from the file starting at bytes offset off
func (df *DataFile) readNBytes(n uint64, offset uint64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, int64(offset))
	return
}
