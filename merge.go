package bitcask_go

import "bitcask-go/data"

// Merge 清理无效数据，生成 Hint 文件

func (db *DB) Merge() error {
	// 如果数据库为空，直接返回
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	// 如果 merge 正在进行中，则直接返回
	if db.isMerging{
		db.mu.Unlock()
		return ErrMergeIsProgress
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()


	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		return err
	}
	// 将当前活跃文件转换为旧的数据文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	// 打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return nil
	}

	// 取出所有需要 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
}