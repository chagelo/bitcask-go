# Simple implementation of bitcask



# Usage

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