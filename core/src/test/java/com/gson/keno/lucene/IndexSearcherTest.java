package com.gson.keno.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;

public class IndexSearcherTest {
    @Test
    public void testSearchDocValues() throws IOException, URISyntaxException {
        String indexDirStr = Paths.get(this.getClass().getResource("").toURI()) + "/indexPosition";
        System.out.println(indexDirStr);
        Directory directory = FSDirectory.open(Paths.get(indexDirStr));
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = new TermQuery(new Term("info", "abc"));
        TopDocs topDocs = searcher.search(query, 1000);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for(ScoreDoc scoreDoc : scoreDocs){
            System.out.println("docId=" + scoreDoc.doc);
            Document d = searcher.doc(scoreDoc.doc);
            List<IndexableField> fields = d.getFields();
            for(IndexableField field : fields){
                System.out.println("field=" + field.name() + " and value = " +
                        d.get(field.name()));
            }
            System.out.println("-------");

        }

    }
}
