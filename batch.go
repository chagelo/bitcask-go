package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// 对应于 db.seqNum 标识提交的 LogRecord 是不属于事务的
const nonTransactionSeqNum uint64 = 0

// 如果事务中某一条提交的数据的 key 就等于这个 txnFinKey 那么可能错误的判断为事务的结束
var txnFinKey = []byte("txn-fin")

// WriteBatch 原子批量写数据，保证原子性
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // 暂存用户写入的数据
}

// NewWriteBatch 初始化 WriteBatch
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	// 如果不是 b+ 树索引，存储事务序列号文件不存在，且不是第一次加载 db 就禁用 writebatch
	if db.options.IndexType == BPlusTree && !db.seqNumFileExists && !db.isInitial{
		panic("cannot use write batch, seq no file no exists")
	}
	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 暂存
	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 数据不存在直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存 LogRecord
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commmit 提交事务，将暂存的数据写到数据文件，并更新内存索引
func (wb *WriteBatch) Commmit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 加锁保证事务提交串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取当前最新的事务序列号
	seqNum := atomic.AddUint64(&wb.db.seqNum, 1)

	// 开始写数据到数据文件中
	// 全部写完之后再更新内存索引
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNum),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}
	/*
	 * 写一条标识事务完成的数据，这里是因为可能存在有些无效事务（事务原子性破坏了）
	 * 在读取的时候看这个事务数据是不是有效的
	 */
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNum),
		Type: data.LogRecordTxnFinished,
	}

	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		
		if oldPos != nil {
			wb.db.reclaimSize += oldPos.Size
		}
	}

	// 清空暂存数据，便于下一次事务 commit
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// key + Seq Number 编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析 LogRecord 的 key，获取实际的 key 和事务号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
