package com.quentinxxz.lucene.kv;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;

/**
 * 类LuceneSotreTest.java的实现描述：LuceneSotreTest类实现描述
 * 
 * @author quentinxxz 2016年10月23日 下午3:20:23
 */
public class LuceneSotreTermQueryTest {

    protected static class KeyField extends Field {

        public static final FieldType TYPE = new FieldType();

        static {
            TYPE.setStored(false);
            TYPE.setIndexed(true);
            TYPE.setOmitNorms(true);
            TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
            TYPE.setTokenized(false);
            TYPE.freeze();
        }

        public KeyField(String name, String key){
            super(name, TYPE);
            fieldsData = key;
        }
    }

    protected static class ValueField extends Field {

        /**
         * Type for numeric DocValues.
         */
        public static final FieldType TYPE = new FieldType();

        static {

            // TYPE.setIndexed(true);
            TYPE.setStored(true);
            // TYPE.setDocValueType(DocValuesType.NUMERIC);
            TYPE.setNumericType(NumericType.INT);// 需要支持范围查询，NumbericType会自动建Trie结构
            TYPE.setOmitNorms(true);
            TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
            TYPE.freeze();
        }

        public ValueField(String name, int value){
            super(name, TYPE);
            fieldsData = value;
        }
    }

    public static void main(String args[]) throws IOException {
        List<String> keys = new ArrayList<String>();
        AtomicInteger index = new AtomicInteger(0);
        File indexPath = new File("/tmp/storeLuceune");

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_45, new WhitespaceAnalyzer(Version.LUCENE_45));
        config.setOpenMode(OpenMode.CREATE);
        config.setRAMBufferSizeMB(64);

        IndexWriter writer = new IndexWriter(FSDirectory.open(indexPath), config);

        try {
            // 生成并插入200w条数据
            Stream.generate(new Supplier<String>() {

                @Override
                public String get() {
                    return LuceneSotreTermQueryTest.getRandomString(20);
                }

            }).limit(2000000).forEach(key -> {
                keys.add(key);
                try {
                    writer.addDocument(LuceneSotreTermQueryTest.getDocument(key, index.getAndIncrement()));
                } catch (Exception e) {
                }

                if (index.get() % 100000 == 0) System.out.println(index + " records has benn inserted");

            });

            writer.forceMerge(1);
        } catch (IOException e) {
            throw new RuntimeException("building index failed. ", e);
        } finally {
            writer.close();
        }

        // 性能测试，100w次查询用时
        long start;
        // mmap方式查询
        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(new MMapDirectory(indexPath)));
        for (int i = 0; i < 5; i++) {
            start = System.currentTimeMillis();
            keys.stream().limit(1000000).forEachOrdered(key -> {

                Query query = new TermQuery(new Term("key", key));
                TopDocs docs;
                try {
                    docs = indexSearcher.search(query, 1);
                    if (docs == null || docs.scoreDocs.length <= 0) // 未找到
                    {
                        System.out.println("not found");
                        return;
                    }

                    Document d = indexSearcher.doc(docs.scoreDocs[0].doc);
                    Integer result = (Integer) d.getField("value").numericValue();// 获得reult
                    // System.out.println(result);

                } catch (Exception e) {
                }

            });
            System.out.println("useed time : " + (System.currentTimeMillis() - start) / 1000.0f + " seconds");
        }
    }

    public static String getRandomString(int length) { // length表示生成字符串的长度
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }

    public static Document getDocument(String key, int value) {
        Document doc = new Document();
        doc.add(new KeyField("key", key));
        doc.add(new ValueField("value", value));
        return doc;
    }

}
