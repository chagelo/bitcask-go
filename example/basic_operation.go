package main

import (
	bitcask "bitcask-go"
	"fmt"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = "/tmp/kv-go"
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	s, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Println("val = ", string(s))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
