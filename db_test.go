package bitcask

import (
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		_ = db.Close()
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

// windows 下文件需要先关闭才能销毁
func closeFiles(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close()
		}
		if db.oldFile != nil {
			for _, file := range db.oldFile {
				_ = file.Close()
			}
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	defer closeFiles(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key 为空
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.value 为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5.写到数据文件进行了转换
	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	// assert.Equal(t, 2, len(db.oldFile))

	// 6.重启后再 Put 数据
	err = db.Close()
	// err = db.activeFile.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	defer closeFiles(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-get")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	defer closeFiles(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常读取一条数据
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.读取一个不存在的 key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// 3.值被重复 Put 后在读取
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4.值被删除后再 Get
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, ErrKeyNotFound, err)

	// 5.转换为了旧的数据文件，从旧的数据文件上获取 value
	for i := 100; i < 500000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	// assert.Equal(t, 2, len(db.oldFile))
	val5, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	// 6.重启后，前面写入的数据都能拿到
	err = db.Close()
	// err = db.activeFile.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	defer closeFiles(db2)
	val6, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db2.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	defer closeFiles(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常删除一个存在的 key
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// 2.删除一个不存在的 key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 3.删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.值被删除之后重新 Put
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.重启之后，再进行校验
	err = db.Close()
	// err = db.activeFile.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	defer closeFiles(db2)
	_, err = db2.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	val2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDB_Close(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-close")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(23))
	assert.Nil(t, err)
}

func TestDB_Sync(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-sync")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(23))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}

func Test_FileLock(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-filelock")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.NotNil(t, db2)
	assert.Nil(t, err)
	err = db2.Close()
	assert.NotNil(t, err)
}

func Test_Stat(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-stat")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 20000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}

	for i := 0; i < 1000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	for i := 3000; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}

	stat := db.Stat()
	t.Log(stat)
}

func TestDB_Backup(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-backup")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 200000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	backupDir, _ := os.MkdirTemp("", "bitcask-backup-2")
	err = db.Backup(backupDir)
	assert.Nil(t, err)

	opts2 := DefaultOptions
	opts2.DirPath = backupDir
	db2, err := Open(opts2)
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
}
