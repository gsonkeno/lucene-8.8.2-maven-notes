package com.gson.keno.lucene.index;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import java.nio.file.Paths;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "Stuff gets printed")
// 东西会被打印出来
public class MyTest2BNumericDocValues extends LuceneTestCase {

    public void testNumerics() throws Exception {
        BaseDirectoryWrapper dir = newFSDirectory(createTempDir("2BNumerics"));
        if (dir instanceof MockDirectoryWrapper) {
            ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
        }

        IndexWriter w = new IndexWriter(dir,
                new IndexWriterConfig(new MockAnalyzer(random()))
                        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
                        .setRAMBufferSizeMB(256.0)
                        .setMergeScheduler(new ConcurrentMergeScheduler())
                        .setMergePolicy(newLogMergePolicy(false, 10))
                        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                        .setCodec(TestUtil.getDefaultCodec()));

        Document doc = new Document();
        NumericDocValuesField dvField = new NumericDocValuesField("dv", 0);
        doc.add(dvField);
        int MAX_DOCS = 10001;
        for (int i = 0; i < MAX_DOCS; i++) {
            dvField.setLongValue(i * 2 + 1);
            w.addDocument(doc);
            if (i % 10 == 0) {
                System.out.println("indexed: " + i);
                System.out.flush();
            }
        }

        w.forceMerge(1);
        w.close();

        System.out.println("verifying...");
        System.out.flush();

        DirectoryReader r = DirectoryReader.open(dir);
        long expectedValue = 0;
        for (LeafReaderContext context : r.leaves()) {
            LeafReader reader = context.reader();
            NumericDocValues dv = reader.getNumericDocValues("dv");
            for (int i = 0; i < reader.maxDoc(); i++) {
                assertEquals(i, dv.nextDoc());
                assertEquals(expectedValue * 2 + 1, dv.longValue());
                expectedValue++;
            }
        }

        r.close();
        dir.close();
    }

    public void testNumerics1() throws Exception {
//        BaseDirectoryWrapper dir = newFSDirectory(createTempDir("2BNumerics"));
//        if (dir instanceof MockDirectoryWrapper) {
//            ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
//        }

        String indexDirStr = Paths.get(this.getClass().getResource("").toURI()) + "/indexPosition";
        Directory dir = FSDirectory.open(Paths.get(indexDirStr));

        IndexWriter w = new IndexWriter(dir,
                new IndexWriterConfig(new MockAnalyzer(random()))
                        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
                        .setRAMBufferSizeMB(256.0)
                        .setMergeScheduler(new ConcurrentMergeScheduler())
                        .setMergePolicy(newLogMergePolicy(false, 10))
                        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                        .setCodec(TestUtil.getDefaultCodec()));

        Document doc = new Document();
        NumericDocValuesField dvField = new NumericDocValuesField("dv", 0);
        doc.add(dvField);
        int PARSE1 = Lucene80DocValuesFormat.NUMERIC_BLOCK_SIZE;
        // 前16384个元素要么是3 要么是4
        for (int i = 0; i < PARSE1; i++) {
            dvField.setLongValue(TestUtil.nextLong(random(), 3, 4));
            w.addDocument(doc);
            if (i % 10 == 0) {
                System.out.println("indexed: " + i);
                System.out.flush();
            }
        }

        // 后面1000个元素在 2000~3000之间
        for (int i = 0; i < 999; i++) {
            dvField.setLongValue(TestUtil.nextLong(random(), 2000, 3000));
            w.addDocument(doc);
        }
        dvField.setLongValue(3000);
        w.addDocument(doc);

        w.forceMerge(1);
        w.close();

        System.out.println("verifying...");
        System.out.flush();
        dir.close();
    }
}
