
# merge

1. 这里会有很多的事务与非事务，merge 之后 transations 还存在吗，或者说具体形式什么样的

>1. merge 之后所有数据都是最新的，所以事务没必要存在了（nonTrasactionNum）

2. 如何 merge 的
>读文件里的 LogRecord 和 索引里面的 LogRecordPos 对比，看是否匹配，索引里面的数据是最新的，如果匹配上，就直接写到 merge 目录里

3. 文件结构，merge完成之后将 hint-index 还有 merge-finished 移动到原目录

```
- bitcask-go-merge
    - *.data
    - hint-index
    - merge-finished
```