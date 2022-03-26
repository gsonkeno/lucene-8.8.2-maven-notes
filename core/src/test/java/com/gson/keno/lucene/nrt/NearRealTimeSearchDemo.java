package com.gson.keno.lucene.nrt;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * http://codepub.cn/2017/12/12/lucene-near-real-time-search/
 * 近实时搜索，未commit的文档也能被搜索到，叫做近实时搜索
 */
public class NearRealTimeSearchDemo {
    public static void main(String[] args) throws IOException {
        RAMDirectory ramDirectory = new RAMDirectory();
        Document document = new Document();
        document.add(new TextField("title", "Doc1", Field.Store.YES));
        document.add(new IntPoint("ID", 1));
        IndexWriter indexWriter = new IndexWriter(ramDirectory, new IndexWriterConfig(new StandardAnalyzer()));
        indexWriter.addDocument(document);
        // 索引的更新是通过indexWriter操作的，所以能被indexSearcher感知到，这里就是近实时搜索，虽然文档未提交
        // 但是DirectoryReader.open(indexWriter)会强制flush，就会生成内存中SegmentCommitInfo信息，这是可被搜索到的
        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(indexWriter));
        int count = indexSearcher.count(new MatchAllDocsQuery());
        if (count > 0) {
            TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), count);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                //即便没有commit，依然能看到Doc1
                System.out.println(indexSearcher.doc(scoreDoc.doc));
            }
            System.out.println("--上方的文档未commit,但是仍然可以被搜索到--");
        }
        System.out.println("=================================");
        document = new Document();
        document.add(new TextField("title", "Doc2", Field.Store.YES));
        document.add(new IntPoint("ID", 2));
        indexWriter.addDocument(document);
        indexWriter.commit();
        count = indexSearcher.count(new MatchAllDocsQuery());
        TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), count);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            //即便后来commit了，依然无法看到Doc2，因为没有重新打开IndexSearcher
            System.out.println(indexSearcher.doc(scoreDoc.doc));
            System.out.println("--即便后来commit了，依然无法看到Doc2，因为没有重新打开IndexSearcher--");
        }
        System.out.println("=================================");
        indexSearcher.getIndexReader().close();
        indexSearcher = new IndexSearcher(DirectoryReader.open(indexWriter));
        count = indexSearcher.count(new MatchAllDocsQuery());
        topDocs = indexSearcher.search(new MatchAllDocsQuery(), count);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            //重新打开IndexSearcher，可以看到Doc1和Doc2
            System.out.println(indexSearcher.doc(scoreDoc.doc));
        }
        System.out.println("--重新打开IndexSearcher，可以看到Doc1和Doc2--");
        System.out.println("=================================");
    }

    /**
     * 搜索端持有indexWriter的情况下守护线程去更新获取最新的IndexSearcher
     * @throws IOException
     */
    @Test
    public void test() throws IOException {
        RAMDirectory ramDirectory = new RAMDirectory();
        Document document = new Document();
        document.add(new TextField("title", "Doc1", Field.Store.YES));
        document.add(new IntPoint("ID", 1));
        IndexWriter indexWriter = new IndexWriter(ramDirectory, new IndexWriterConfig(new StandardAnalyzer()));
        indexWriter.addDocument(document);
        indexWriter.commit();
        SearcherManager searcherManager = new SearcherManager(indexWriter, null);
        //当没有调用者等待指定的generation的时候，必须要重新打开时间间隔5s，言外之意，如果有调用者在等待指定的generation，则只需等0.25s
        //防止不断的重新打开，严重消耗系统性能，设置最小重新打开时间间隔0.25s
        ControlledRealTimeReopenThread<IndexSearcher> controlledRealTimeReopenThread = new ControlledRealTimeReopenThread<>(indexWriter,
                searcherManager, 5, 0.25);
        //设置为后台线程
        controlledRealTimeReopenThread.setDaemon(true);
        controlledRealTimeReopenThread.setName("controlled reopen thread");
        controlledRealTimeReopenThread.start();
        int count = 0;
        IndexSearcher indexSearcher = searcherManager.acquire();
        try {
            //只能看到Doc1
            count = indexSearcher.count(new MatchAllDocsQuery());
            if (count > 0) {
                TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), count);
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    System.out.println(indexSearcher.doc(scoreDoc.doc));
                }
            }
            System.out.println("=================================");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            searcherManager.release(indexSearcher);
        }
        document = new Document();
        document.add(new TextField("title", "Doc2", Field.Store.YES));
        document.add(new IntPoint("ID", 2));
        indexWriter.addDocument(document);
        try {
            TimeUnit.SECONDS.sleep(6);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //休息6s之后，即使没有commit，依然可以搜索到Doc2，因为ControlledRealTimeReopenThread会刷新SearchManager
        indexSearcher = searcherManager.acquire();
        try {
            count = indexSearcher.count(new MatchAllDocsQuery());
            if (count > 0) {
                TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), count);
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    System.out.println(indexSearcher.doc(scoreDoc.doc));
                }
            }
            System.out.println("=================================");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            searcherManager.release(indexSearcher);
        }
        document = new Document();
        document.add(new TextField("title", "Doc3", Field.Store.YES));
        document.add(new IntPoint("ID", 3));
        long generation = indexWriter.addDocument(document);
        try {
            //当有调用者等待某个generation的时候，只需要0.25s即可重新打开
            controlledRealTimeReopenThread.waitForGeneration(generation);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        indexSearcher = searcherManager.acquire();
        try {
            count = indexSearcher.count(new MatchAllDocsQuery());
            if (count > 0) {
                TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), count);
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    System.out.println(indexSearcher.doc(scoreDoc.doc));
                }
            }
            System.out.println("=================================");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            searcherManager.release(indexSearcher);
        }
    }

    @Test
    public void testOpenIfChange() throws IOException {
        RAMDirectory ramDirectory = new RAMDirectory();
        Document document = new Document();
        document.add(new TextField("title", "Doc1", Field.Store.YES));
        document.add(new IntPoint("ID", 1));
        IndexWriter indexWriter = new IndexWriter(ramDirectory, new IndexWriterConfig(new StandardAnalyzer()));
        indexWriter.addDocument(document);
        indexWriter.commit();
        DirectoryReader directoryReader = DirectoryReader.open(ramDirectory);
        IndexSearcher indexSearcher = new IndexSearcher(directoryReader);
        document = new Document();
        document.add(new TextField("title", "Doc2", Field.Store.YES));
        document.add(new IntPoint("ID", 2));
        indexWriter.addDocument(document);
        //即使commit，搜索依然不可见，需要重新打开reader
        indexWriter.commit();
        //只能看到Doc1
        int count = indexSearcher.count(new MatchAllDocsQuery());
        if (count > 0) {
            TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), count);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                System.out.println(indexSearcher.doc(scoreDoc.doc));
            }
        }
        System.out.println("=================================");
        //如果发现有新数据更新，则会返回一个新的reader
        DirectoryReader newReader = DirectoryReader.openIfChanged(directoryReader);
        if (newReader != null) {
            indexSearcher = new IndexSearcher(newReader);
            directoryReader.close();
        }
        count = indexSearcher.count(new MatchAllDocsQuery());
        if (count > 0) {
            TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), count);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                System.out.println(indexSearcher.doc(scoreDoc.doc));
            }
        }
        System.out.println("=================================");
    }
}
