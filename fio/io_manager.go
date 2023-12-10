package fio

const DataFilePerm = 0644


// IOManager 抽象 IO 管理接口，可以接入不同的 IO 类型，比如 mmap，当前只支持标准 IO
type IOManager interface {
	// Read 从文件的给定位置读取
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error
	Close() error

	// Size 获取到文件大小
	Size() (uint64, error)
}

// NewIOManager 初始化 IOManager, 目前只支持 FileIO，后续比如 mmap
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
	// return NewMmapIOManager(fileName)
}