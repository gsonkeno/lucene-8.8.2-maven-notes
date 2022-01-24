package com.gson.keno.lucene.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class PersistentSnapshotDeletionPolicyTest {
    private Directory directory ;
    private Analyzer analyzer = new WhitespaceAnalyzer();
    private IndexWriterConfig conf = new IndexWriterConfig(analyzer);
    private IndexWriter oldIndexWriter;
    private PersistentSnapshotDeletionPolicy persistentSnapshotDeletionPolicy;

    {
        try {
            FileOperation.deleteFile("./data");
            directory = FSDirectory.open(Paths.get("./data"));
            persistentSnapshotDeletionPolicy = new PersistentSnapshotDeletionPolicy(NoDeletionPolicy.INSTANCE, directory);
            conf.setIndexDeletionPolicy(persistentSnapshotDeletionPolicy);
            oldIndexWriter = new IndexWriter(directory, conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public void doIndex() throws Exception {

        FieldType type = new FieldType();
        type.setStored(true);
        type.setStoreTermVectors(true);
        type.setStoreTermVectorPositions(true);
        type.setStoreTermVectorPayloads(true);
        type.setStoreTermVectorOffsets(true);
        type.setTokenized(true);
        type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

        Document doc;
        // 文档0
        doc = new Document();
        doc.add(new Field("author", "Lucy", type));
        doc.add(new NumericDocValuesField("docValuesField", 8));
        oldIndexWriter.addDocument(doc);
        oldIndexWriter.commit();
        // 文档1
        doc = new Document();
        doc.add(new Field("author", "Lucy", type));
        doc.add(new NumericDocValuesField("docValuesField", 3));
        oldIndexWriter.addDocument(doc);
        oldIndexWriter.commit();
        IndexCommit indexCommit = persistentSnapshotDeletionPolicy.snapshot();

        // 文档2
        doc = new Document();
        doc.add(new Field("author", "Jay", type));
        doc.add(new IntPoint("pointValue", 3, 4, 5));
        oldIndexWriter.addDocument(doc);
        oldIndexWriter.deleteDocuments(new Term("author", "Lucy"));
        oldIndexWriter.commit();
        oldIndexWriter.close();

        IndexWriterConfig newConf = new IndexWriterConfig(analyzer);
        newConf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        newConf.setIndexCommit(indexCommit);
        IndexWriter newIndexWriter = new IndexWriter(directory, newConf);
//        // 文档3
//        doc = new Document();
//        doc.add(new Field("author", "JayOne", type));
//        doc.add(new IntPoint("pointValue",  4, 5,6));
//        newIndexWriter.addDocument(doc);
//        newIndexWriter.commit();
//        newIndexWriter.close();

        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = new TermQuery(new Term("author", "Jay"));
        // TopDocs topDocs = searcher.search(query, 1000);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 100);
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
    public static void main(String[] args) throws Exception{
        PersistentSnapshotDeletionPolicyTest test = new PersistentSnapshotDeletionPolicyTest();
        test.doIndex();
    }
}
