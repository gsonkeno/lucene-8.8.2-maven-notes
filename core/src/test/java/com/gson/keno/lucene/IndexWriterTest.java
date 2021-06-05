package com.gson.keno.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class IndexWriterTest {

    IndexWriter getIndexWriter() throws IOException, URISyntaxException {
        String indexDirStr = Paths.get(this.getClass().getResource("").toURI()) + "/indexPosition";
        System.out.println(indexDirStr);
        Directory dir = FSDirectory.open(Paths.get(indexDirStr));

        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        // Create a new index in the directory, removing any
        // previously indexed documents:
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        //是否打印输出
        iwc.setInfoStream(new PrintStreamInfoStream(System.out));
        //是否生成复合索引文件
        iwc.setUseCompoundFile(false);

        IndexWriter writer = new IndexWriter(dir, iwc);
        return writer;
    }

    /**
     * 测试只增加倒排索引
     */
    @Test
    public void testCreateInvertFieldDocIndex() throws IOException, URISyntaxException {
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
        doc.add(new TextField("info", "study play football ! hi study boys", Field.Store.YES));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("info", "hi, every one, good play", Field.Store.YES));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("info", "play basketball is one good interest", Field.Store.YES));
        writer.addDocument(doc);

        writer.commit();
        writer.close();
    }

}
