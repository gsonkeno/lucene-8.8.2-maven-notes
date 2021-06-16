package com.gson.keno.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
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
     * 测试只增加倒排索引,索引选项为{@link org.apache.lucene.index.IndexOptions#DOCS_AND_FREQS_AND_POSITIONS}
     * 不包含偏移量
     */
    @Test
    public void testCreateInvertFieldDocIndex() throws IOException, URISyntaxException {
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
        doc.add(new TextField("info", "study play football ! study", Field.Store.YES));
        doc.add(new TextField("school", "JiNan university", Field.Store.YES));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("info", "play hi, every one, good play study", Field.Store.YES));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("info", "play basketball is one good interest", Field.Store.YES));
        writer.addDocument(doc);

        writer.commit();
        writer.close();
    }

    @Test
    public void testBlockTreeTermsWriter() throws IOException, URISyntaxException {
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
        doc.add(new TextField("info", "study play foot ! study football studying", Field.Store.YES));
        writer.addDocument(doc);

        writer.commit();
        writer.close();
    }

    /**
     * 测试只增加倒排索引,索引选项为{@link org.apache.lucene.index.IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
     * 包含偏移量
     */
    @Test
    public void testCreateInvertFieldDocIndexUseDOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS() throws IOException, URISyntaxException {
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();

        FieldType fieldType = new FieldType();
        fieldType.setStored(true);
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        fieldType.setTokenized(true);
        fieldType.freeze();

        doc.add(new Field("info", "play football", fieldType));
        doc.add(new Field("info", "hello", fieldType));
        doc.add(new Field("info", "happy", fieldType));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(new Field("info", "hi, every one, good play study", fieldType));
        writer.addDocument(doc);

        doc = new Document();
        doc.add(new Field("info", "play basketball is one good interest", fieldType));
        writer.addDocument(doc);

        writer.commit();
        writer.close();
    }
}
