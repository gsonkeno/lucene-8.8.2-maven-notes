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

    @Test
    public void testBlockTreeTermsWriter1() throws IOException, URISyntaxException {
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
        doc.add(new TextField("info",
                  " matter matte matron matrix matrimony matriculate !" +
                        " melody  melodramatic melodrama  melodious  melodic  mellow " +
                        " mellifluous  meliorism  melee meld Melbourne   melancholy " +
                        " megalopolis megalomania megalith megacycle  meeting meet meek " +
                        " meekly  medium  medulla  medley  most  moss  mostly", Field.Store.YES));
        writer.addDocument(doc);

        writer.commit();
        writer.close();
    }

    @Test
    public void testBlockTreeTermsWriter2() throws IOException, URISyntaxException {
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
        doc.add(new TextField("info",
                   " a aa ab aba abb abc abd abe abf abg abh abi abj abk abl abm abn" +
                        " abo abp abq abr abs abt abu abv abw abx aby abz" +
                        " ac  aca  acb ad ae af az ac ", Field.Store.YES));
        writer.addDocument(doc);

        writer.commit();
        writer.close();
    }

    @Test
    public void testBlockTreeTermsWriter3() throws IOException, URISyntaxException {
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
//        doc.add(new TextField("info", "abc, acc, acd, acea, aceb, acee, adff",
//                Field.Store.YES));
        doc.add(new TextField("info", "abc, acc, acea, aceb, acee, adff",
                Field.Store.YES));
        writer.addDocument(doc);

        writer.commit();
        writer.close();
    }

    @Test
    public void testBlockTreeTermsWriter4() throws IOException, URISyntaxException {
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
//        doc.add(new TextField("info", "abc, acc, acd, acea, aceb, acee, adff",
//                Field.Store.YES));
        doc.add(new TextField("info", "abc, acc, acea, aceb, acec, aced, acee, acef, aceg, aceh, acei, acej, acek," +
                "acel, acem, acen, aceo, acep, aceq, acer, aces, acet, aceu, acev, acew, acex, acey,acez, d",
                Field.Store.YES));
        writer.addDocument(doc);

        writer.commit();
        writer.close();
    }

    @Test
    public void testBlockTreeTermsWriter5() throws IOException, URISyntaxException {
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
//        doc.add(new TextField("info", "abc, acc, acd, acea, aceb, acee, adff",
//                Field.Store.YES));
        doc.add(new TextField("info", "abc, acc, acea, aceb, acec, aced, acee, acef, aceg, aceh, acei, acej, acek," +
                "acel, acem, acen, aceo, acep, aceq, acer, aces, acet, aceu, acev, acew, acex, acey, acez, " +
                "acf, acg, ach,aci, acj, ack, acl, acm, acn, aco, acoa, acob, acoc, acod, acoe, acof, acog, acoh," +
                "acoi, acoj, acok, acol, acom, acon, acoo, acop, acoq, acor, acos, acot, acou, acov, acow, acox, acoy," +
                "acoz, ad",
                Field.Store.YES));
        writer.addDocument(doc);

        writer.commit();
        writer.close();
    }

    @Test
    public void testBlockTreeTermsWriter6() throws IOException, URISyntaxException {
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
//        doc.add(new TextField("info", "abc, acc, acd, acea, aceb, acee, adff",
//                Field.Store.YES));
        doc.add(new TextField("info", "abc, acc, acea, aceb, acec, aced, acee, acef, aceg, aceh, acei, acej, acek," +
                "acel, acem, acen, aceo, acep, aceq, acer, aces, acet, aceu, acev, acew, acex, acey, acez, " +
                "acf, acg, ach,aci, acj, ack, ad",
                Field.Store.YES));
        writer.addDocument(doc);

        writer.commit();
        writer.close();
    }

    @Test
    public void testBlockTreeTermsWriter7() throws IOException, URISyntaxException {
        IndexWriter writer = getIndexWriter();
        Document doc = new Document();
        doc.add(new TextField("info", "a,b,c,d,e", Field.Store.YES));
        writer.addDocument(doc);

        writer.commit();
        writer.close();
    }

    @Test
    public void testBlockTreeTermsWriter8() throws IOException, URISyntaxException {
        IndexWriter writer = getIndexWriter();
        for(int i =0 ; i< 300; i++){
            Document doc = new Document();
            doc.add(new TextField("info", i%2 == 0? "a":"b", Field.Store.YES));
            writer.addDocument(doc);
        }
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
