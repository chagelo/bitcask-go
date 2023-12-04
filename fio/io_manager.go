package fio

const DataFilePerm = 0644


// IOManager 抽象 IO 管理接口
type IOManager interface {
	// Read 从文件的给定位置读取
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error
	Close() error
}