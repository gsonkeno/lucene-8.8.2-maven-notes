##
codes migrated from lucene 8.8.2 code and making it as
one maven project

learning notes based on lucene 8.8.2 

## main class
- SegmentInfo --> SegmentCommitInfo --> SegmentInfos
> 前者被后者所持有，靠后的类拥有的索引信息比前者多。<br>

> 我们知道一个segment_[N]文件的内部信息结构中，会有一个SegmentCommitInfo信息的列表，
> 列表的长度numSegments也是segment_[N]文件内部结构中的一个字段。<br>
> 
> 一般的indexWriter在commit文档后，就会对应一个SegmentCommitInfo。每commit一次，就
> 会产生一个SegmentCommitInfo。

- SegmentReader
>