package com.gson.keno.lucene.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * dvd, dvm 索引文件测试
 */
public class TestDvdDvm {
    private String indexDir = new File(System.getProperty("user.dir")).getParentFile() + "/testDvdDvm";

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
    public void testIndex() throws IOException {
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
        long l1 = indexWriter.addDocument(doc);
        System.out.println("<==>addDocument no =" + l1);

        long l2 = indexWriter.updateNumericDocValue(new Term("content", "h"), "price", 1);
        System.out.println("<==>updateNumericDocValue no =" + l2);

        indexWriter.commit();
        indexWriter.close();

//        IndexReader reader = DirectoryReader.open(directory);
//        IndexSearcher searcher = new IndexSearcher(reader);
//        Query query = new MatchAllDocsQuery();
//        TopDocs topDocs = searcher.search(query, 1000);
//        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
//        for (ScoreDoc scoreDoc : scoreDocs) {
//            Document d = searcher.doc(scoreDoc.doc);
//            System.out.println(scoreDoc.doc + "-->" + d);
//            System.out.println("<=======>");
//        }
    }
}
