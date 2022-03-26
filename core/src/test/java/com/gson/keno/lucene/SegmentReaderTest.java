package com.gson.keno.lucene;

import com.gson.keno.lucene.index.FileOperation;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class SegmentReaderTest {
    private String indexDir = new File(System.getProperty("user.dir")).getParentFile() + "/testSegmentReader";

    private Directory directory;

    @Before
    public void before() {
        try {
            // 测试append属性时，不必删除文件夹
            FileOperation.deleteFile(indexDir);
            directory = new MMapDirectory(Paths.get(indexDir));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSegmentReader() throws IOException {
        Analyzer analyzer = new WhitespaceAnalyzer();
        IndexWriterConfig conf = new IndexWriterConfig(analyzer);
        // 测试append模式
        // conf.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

        IndexWriter indexWriter = new IndexWriter(directory, conf);
        Document doc;
        // doc0
        doc = new Document();
        doc.add(new TextField("content", "h", Field.Store.YES));
        doc.add(new TextField("content", "h", Field.Store.YES));
        indexWriter.addDocument(doc);
        // doc1
        doc = new Document();
        doc.add(new TextField("content", "ma", Field.Store.YES));
        indexWriter.addDocument(doc);
        // doc2
        doc = new Document();
        doc.add(new TextField("content", "kz", Field.Store.YES));
        indexWriter.addDocument(doc);
        // doc3
        doc = new Document();
        doc.add(new TextField("content", "ola", Field.Store.YES));
        indexWriter.addDocument(doc);
        //doc4
        doc = new Document();
        doc.add(new TextField("content", "h", Field.Store.YES));
        indexWriter.addDocument(doc);
        //doc5
        doc = new Document();
        doc.add(new TextField("content", "olq", Field.Store.YES));
        indexWriter.addDocument(doc);
        //doc6
        doc = new Document();
        doc.add(new TextField("content", "zjs", Field.Store.YES));
        indexWriter.addDocument(doc);
        //doc7
        doc = new Document();
        doc.add(new TextField("content", "f", Field.Store.YES));
        indexWriter.addDocument(doc);
        //doc8
        doc = new Document();
        doc.add(new TextField("content", "k slz", Field.Store.YES));
        indexWriter.addDocument(doc);
        //doc9
        doc = new Document();
        doc.add(new TextField("content", "pa qz", Field.Store.YES));
        indexWriter.addDocument(doc);

        // doc0,doc4,doc7被删除
        indexWriter.deleteDocuments(new Term("content", "h"));
        indexWriter.deleteDocuments(new Term("content", "f"));
        indexWriter.commit();

        // 读取
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = new MatchAllDocsQuery();
        TopDocs topDocs = searcher.search(query, 1000);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            Document d = searcher.doc(scoreDoc.doc);
            System.out.println(scoreDoc.doc + "-->" + d);
            System.out.println("<=======>");
        }
    }

    @Test
    public void testUpdateDocValues() throws IOException {
        Analyzer analyzer = new WhitespaceAnalyzer();
        IndexWriterConfig conf = new IndexWriterConfig(analyzer);
        // 是否使用复合索引
        conf.setUseCompoundFile(false);

        // 测试append模式
        // conf.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

        IndexWriter indexWriter = new IndexWriter(directory, conf);
        Document doc;
        // doc0
        doc = new Document();
        doc.add(new NumericDocValuesField("price", 2));
        doc.add(new TextField("content", "h", Field.Store.YES));
        indexWriter.addDocument(doc);

        indexWriter.updateNumericDocValue(new Term("content","h"), "price", 3);
        indexWriter.commit();
        indexWriter.close();

        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = new MatchAllDocsQuery();
        TopDocs topDocs = searcher.search(query, 1000);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            Document d = searcher.doc(scoreDoc.doc);
            System.out.println(scoreDoc.doc + "-->" + d);
            System.out.println("<=======>");
        }
    }
}
