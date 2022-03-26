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

- SegmentCoreReaders
> 一个段的核心读对象，其持有着读一个Segment过程中共享的不变的reader。

> SegmentCoreReaders中包含的索引信息中StoredFieldsReader、TermVectorsReader、PointsReader、NormsProducer是不会发生更改的内容，
> 使得SegmentCoreReaders能够被复用。

> 如果在未来的操作，该段中的索引信息发生更改，那么段中变更的索引信息会以其他索引文件来描述，
> 这便是索引文件之liv、索引文件值.dvm、.dvd、索引文件之fnm。
>
>文档被删除：被删除的文档通过索引文件之liv来描述
> 
> 文档被更新：文档的更新实际是先删除，后添加的过程，如果是更新DocValues，那么使用索引文件值.dvm、.dvd、索引文件之fnm来描述
