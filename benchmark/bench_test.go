package benchmark

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	bitcask "bitcask-go"
	"bitcask-go/utils"
)

var db *bitcask.DB

func openDB() func() {
	options := bitcask.DefaultOptions
	options.DirPath = "/tmp/bitcask-go"

	var err error
	db, err = bitcask.Open(options)
	if err != nil {
		panic(err)
	}

	return func() {
		_ = db.Close()
		_ = os.RemoveAll(options.DirPath)
	}
}

func Benchmark_Put(b *testing.B) {
	closer := openDB()
	defer closer()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	closer := openDB()
	defer closer()

	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(r.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	closer := openDB()
	defer closer()

	b.ResetTimer()
	b.ReportAllocs()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(r.Int()))
		assert.Nil(b, err)
	}
}
