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
> SegmentCommitInfo用来描述一个段元数据（metadata）。它是索引文件segments_N的字段
> 
> 一般的indexWriter在commit文档后，就会对应一个SegmentCommitInfo。每commit一次，就
> 会产生一个SegmentCommitInfo。

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

- SegmentReaders
> 持有SegmentCoreReaders的引用,其涵盖着一个段变更过程中保持不变的关键信息，这样在Directory.open(directory)时开销不至于那么大。

- ReaderPool
> 该类持有一个或多个SegmentReader对象的引用，并且是shared SegmentReader，
> share描述的是在flush阶段、合并阶段、NRT等不同的场景中都共用ReaderPool对象中
> 的SegmentReader。
> 
>另外IndexWriter还会使用这些shared SegmentReaders来实现例如作用（apply）删除信息、
> 执行段的合并、NRT搜索。

## index file introduce
- 索引文件fnm
> 描述的是索引中的字段(FieldInfo)信息，包括但不限于域字段的个数，以及每个域字段的信息。
> 
> 每个域字段包括域字段的名称(如"author","content")，<br>
> 域字段的编号(递增), <br>
> 是否存储词向量(termVector), <br>
> 是否存储norm值(用于打分), <br>
> 是否带有payload, <br>
> 是否为软删除域(soft delete field), <br>
> 域字段的索引方式(NONE、DOCS、DOCS_AND_FREQS、
> DOCS_AND_FREQS_AND_POSITIONS、DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS）
> 以及其他信息

