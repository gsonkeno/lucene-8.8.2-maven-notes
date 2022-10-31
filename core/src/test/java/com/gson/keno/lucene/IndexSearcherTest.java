package com.gson.keno.lucene;

import com.gson.keno.lucene.index.FileOperation;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;

public class IndexSearcherTest {
    private String indexDir = new File(System.getProperty("user.dir")).getParentFile() + "/gsdata";
    private Directory directory;
    @Before
    public void before(){
        try {
            // 测试append属性时，不必删除文件夹
             FileOperation.deleteFile(indexDir);
             directory = new MMapDirectory(Paths.get(indexDir));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testFieldStored() throws IOException {
        Analyzer analyzer = new WhitespaceAnalyzer();
        IndexWriterConfig conf = new IndexWriterConfig(analyzer);
        // 测试append模式
        // conf.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

        conf.setUseCompoundFile(false);

        IndexWriter indexWriter = new IndexWriter(directory, conf);
        Document doc = new Document();
        doc.add(new TextField("title", "how lucene work", Field.Store.YES));
        doc.add(new TextField("author", "Mr Yang", Field.Store.NO));

        indexWriter.addDocument(doc);
        // 执行commit()操作后，生成segments_1文件
        indexWriter.commit();

        // indexWriter关闭，释放索引文件锁
        indexWriter.close();

        // ==============开始读=================
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        // 查询命中term的文档
        Query query = new TermQuery(new Term("author", "Yang"));
        TopDocs topDocs = searcher.search(query, 1);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for(ScoreDoc scoreDoc : scoreDocs){
            StringBuilder sb = new StringBuilder().append("<===docId = " + scoreDoc.doc);
            Document d = searcher.doc(scoreDoc.doc);
            List<IndexableField> fields = d.getFields();
            for(IndexableField field : fields){
                sb.append(", ").append(field.name()).append(" = ").append(field.stringValue());
            }
            sb.append("===>");
            // print <===docId = 0, title = how lucene work===>
            System.out.println(sb);
        }
    }
    @Test
    public void testSearchDocValues() throws IOException {
        Analyzer analyzer = new WhitespaceAnalyzer();
        IndexWriterConfig conf = new IndexWriterConfig(analyzer);
        // 测试append模式
        // conf.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

        IndexWriter indexWriter = new IndexWriter(directory, conf);
        Document doc = new Document();
        doc.add(new TextField("title", "how lucene work", Field.Store.NO));

        indexWriter.addDocument(doc);
        // 执行commit()操作后，生成segments_1文件
        indexWriter.commit();

        // indexWriter关闭，释放索引文件锁
        indexWriter.close();

        // ==============开始读=================
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        // 查询索引目录中索引对应的所有doc
        Query query = new MatchAllDocsQuery();
        TopDocs topDocs = searcher.search(query, 1000);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for(ScoreDoc scoreDoc : scoreDocs){
            System.out.println("docId=" + scoreDoc.doc);
            Document d = searcher.doc(scoreDoc.doc);
            List<IndexableField> fields = d.getFields();
            for(IndexableField field : fields){
                System.out.println("field=" + field.name() + " and value = " + d.get(field.name()));
            }
            System.out.println("-------");
        }
    }
}
