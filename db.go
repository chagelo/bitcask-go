package bitcask_go

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"

	"bitcask-go/data"
	"bitcask-go/fio"
	"bitcask-go/index"
	"bitcask-go/utils"
)

const (
	seqNumKey    = "seq.num"
	fileLockName = "flock"
)

// DB bitcask 存储引擎实例
type DB struct {
	options          Options
	mu               *sync.RWMutex
	fileIds          []int                     // 文件 id，只能用于在加载文件索引的时候使用，不能用于其他
	activeFile       *data.DataFile            // 当前活跃数据文件用于写入
	olderFiles       map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index            index.Indexer             // 内存索引
	seqNum           uint64                    // 事务序列号，全局递增
	isMerging        bool                      // 是否正在 merge，只允许有一个merge 操作
	seqNumFileExists bool                      // 存储事务序列号的文件是否存在
	isInitial        bool                      // 是否第一次初始化此数据目录
	fileLock         *flock.Flock              // 文件锁保证多线程之间的互斥
	bytesWrite       uint                      // 累计写了多少个字节
	reclaimSize      uint64                    // 标识有多少数据是无效的
}

// Stat 存储引擎统计信息
type Stat struct {
	KeyNum          uint   // key 的总数量
	DataFileNum     uint   // 数据文件总量
	ReclaimableSize uint64 // 可以进行 merge 回收的数据量
	DiskSize        uint64 // 数据目录所占磁盘空间大小
}

// Open 打开 bitcast 存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool

	// 判断目录是否存在，不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		isInitial = true
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	// 加载 merge 数据目录
	if err := db.logMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件，读取 hint file
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// B+ 索引
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNum(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}

		return db, nil
	}

	// 从 hint 索引文件中加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	// 重置 IO 类型，重置的原因是当前 mmap 的写和 sync 没有实现
	if db.options.MMapAtStartup {
		if err := db.resetIoType(); err != nil {
			return nil, err
		}
	}

	return db, nil
}

// 构造的 DB 的写操作，key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// key 是否有效
	if len(value) == 0 {
		return ErrKeyIsEmpty
	}

	// 有效的 key-value 数据，构造 LogRecord 结构体
	log_record := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNum),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 对磁盘进行写，并返回索引
	pos, err := db.appendLogRecordWithLock(log_record)
	if err != nil {
		return err
	}

	// 更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += oldPos.Size
	}
	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存数据结构中取出 key 对应的索引信息
	logRecordPos := db.index.Get(key)

	// 如果 key 不在内存索引中，说明 key 不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 从数据文件中获取 value
	return db.getValuesByPosition(logRecordPos)
}

func (db *DB) Delete(key []byte) error {
	// 空的 key
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先检查 key 是否存在，不存在直接返回，不直接返回的情况下后续会导致日志出现很多无效的不存在 key 的记录
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 LogRecord，标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNum),
		Type: data.LogRecordDeleted,
	}

	// 写入到数据文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}
	db.reclaimSize += pos.Size

	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	
	if oldPos != nil {
		db.reclaimSize += oldPos.Size
	}

	return nil
}

func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to  unlock the directory, %v", err))
		}
	}()

	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 保存当前事务序列号
	seqNumFile, err := data.OpenSeqNumFile(db.options.DirPath)
	if err != nil {
		return err
	}

	record := &data.LogRecord{
		Key:   []byte(seqNumKey),
		Value: []byte(strconv.FormatUint(db.seqNum, 10)),
	}

	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNumFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNumFile.Sync(); err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Stat 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size: %v", err))
	}

	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize, // TODO
	}
}

// Backup 备份数据库，将数据库拷贝到新的目录中，旨在数据恢复
func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}

// 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

// ListKeys 获取数据库中所有的 Key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()

	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}

	return keys
}

// Fold 获取所有数据，并执行用户指定的操作，函数返回 false 时终止操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator((false))
	defer iterator.Close()

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValuesByPosition(iterator.Value())
		if err != nil {
			return err
		}

		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// 根据数据文件索引获取对应的 value
func (db *DB) getValuesByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	// 根据文件 id 找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据数据偏移读取对应数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 向活跃文件写数据
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	// 判断当前活跃文件是否存在，不存在则初始化数据文件，数据库没有写入的时候没有文件生成
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入的数据已经到达了活跃文件的阈值，则关闭活跃文件，并打开新文件写
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(size)

	// 根据用户配置决定是否持久化，每 BytesPerSync 个字节持久化一次
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}

	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff, Size: size}
	return pos, nil
}

// 设置当前活跃文件
// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	// 实际情况中，为了保证安全获取可以对文件 id 进行 hash
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	// 遍历目录中的所有文件，找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 0000001.data
			splitNames := strings.Split(entry.Name(), ".")
			fileID, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileID)
		}
	}

	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每个文件 id，打开对应的数据文件，找到 id 最大的，就是活跃文件
	for i, fid := range fileIds {
		ioType := fio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}

		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}

		// 当前活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else { //说明是旧的文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += pos.Size
		} else {
			oldPos = db.index.Put(key, pos)
		}

		// TODO: correct? if oldPos != nil and type == deleted
		if oldPos != nil {
			db.reclaimSize += oldPos.Size
		}
	}

	// 暂存事务数据，需要存储 LogRecord 和位置索引信息
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	// 更新最新的事务序列号
	var currentseqNum = nonTransactionSeqNum

	// 遍历所有的文件 id，处理文件中的内容
	for i, fileId := range db.fileIds {
		var fileId = uint32(fileId)
		// 如果比最近未参与 merge 的文件 id 更小，则说明已经从 Hint 文件中加载索引了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset uint64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			// 两种错误，异常或者读到文件末尾
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构建内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset, Size: size}

			// 解析 key, 拿到事务号
			realKey, seqNum := parseLogRecordKey(logRecord.Key)
			if seqNum == nonTransactionSeqNum {
				// 非事务操作，直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 事务完成，对应的 seqNum 的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNum] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNum)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNum] = append(transactionRecords[seqNum], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号
			if seqNum > currentseqNum {
				currentseqNum = seqNum
			}

			// 读取下一个 LogRecord
			offset += size
		}
		// 如果是当前活跃文件，更新这个文件的 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新 db 事务序列号
	db.seqNum = currentseqNum
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
	}

	return nil
}

// B+ 树情况下读取文件存储的事务序列号
func (db *DB) loadSeqNum() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNumFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNumFile, err := data.OpenSeqNumFile(db.options.DirPath)
	if err != nil {
		return err
	}

	record, _, err := seqNumFile.ReadLogRecord(0)
	if err != nil {
		return err
	}

	if string(record.Key) != data.SeqNumFileName {
		panic("transactions sequnence number file corrupted!")
	}

	seqNum, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}

	db.seqNum = seqNum
	db.seqNumFileExists = true

	return os.Remove(fileName)
}

func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}

	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}
