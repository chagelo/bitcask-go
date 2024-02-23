# Simple implementation of bitcask



# Usage

## interface

func|usage
-|-
DB.Get(k)| get value
DB.Put(k, v)| put key value
DB.Delete(k)| delete key value
DB.Close()| close database engine
DB.Stat()| get database engine info
DB.Backup(dir)| backup database copy data to new directory
DB.Sync()| sync datafile to disk
DB.ListKeys()| list all keys
DB.Fold(fn(k, v))|
DB.Merge()|clear invalid data

## launch redis server

```bash
cd redis/cmd
go build
./cmd
```

## launch redis client

usage follow redis RESP protocol

```bash
cd /temp
redis-cli

set a 100
# OK
get a
# 100

set key bitcask
# OK
get key
# bitcask

get bitcask
# nil
```