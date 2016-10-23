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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;

/**
 * 类LuceneDocValuesTest.java的实现描述：use DocValues as value store
 * 
 * @author quentinxxz 2016年10月23日 下午3:30:19
 */
public class LuceneDocValuesTest {

    protected static class KeyField extends Field {

        /**
         * Type for numeric DocValues.
         */
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

            TYPE.setIndexed(true);
            TYPE.setStored(false);
            TYPE.setDocValueType(DocValuesType.NUMERIC);
            TYPE.setNumericType(NumericType.INT);
            TYPE.setOmitNorms(true);
            TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
            TYPE.freeze();
        }

        public ValueField(String name, long value){
            super(name, TYPE);
            fieldsData = value;
        }
    }

    public static void main(String args[]) throws IOException {
        List<String> keys = new ArrayList<String>();
        AtomicInteger index = new AtomicInteger(0);
        File indexPath = new File("/tmp/docValueLuceune");

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_45, new WhitespaceAnalyzer(Version.LUCENE_45));
        config.setOpenMode(OpenMode.CREATE);
        config.setRAMBufferSizeMB(64);

        IndexWriter writer = new IndexWriter(FSDirectory.open(indexPath), config);

        try {
            // 生成并插入200w条数据
            Stream.generate(new Supplier<String>() {

                @Override
                public String get() {
                    return LuceneDocValuesTest.getRandomString(20);
                }

            }).limit(2000000).forEach(key -> {
                keys.add(key);
                try {
                    writer.addDocument(LuceneDocValuesTest.getDocument(key, index.getAndIncrement()));
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
        List<TermsEnum> termsEnumList;
        List<NumericDocValues> docValuesList;
        IndexReader indexReader = DirectoryReader.open(new MMapDirectory(indexPath));

        docValuesList = new ArrayList<NumericDocValues>();
        termsEnumList = new ArrayList<TermsEnum>();// 事先初始化termsEnumList，会有多线程问题，当多线程查询时，请用ThreadLocal封装
        for (AtomicReaderContext context : indexReader.leaves()) {
            docValuesList.add(context.reader().getNumericDocValues("value"));
            termsEnumList.add(context.reader().terms("key").iterator(null));
        }

        // mmap方式查询
        for (int i = 0; i < 10; i++) {
            start = System.currentTimeMillis();
            keys.stream().limit(1000000).forEachOrdered(key -> {

                Term term = new Term("key", key);

                try {
                    for (int l = 0; l < termsEnumList.size(); l++) {
                        NumericDocValues docValues = docValuesList.get(l);
                        TermsEnum termsEnum = termsEnumList.get(l);

                        if (termsEnum.seekExact(term.bytes()) == false) continue;
                        DocsEnum docs = termsEnum.docs(null, null);
                        int docId = docs.nextDoc();
                        Integer reutlt = (int) docValues.get(docId);
                        // System.out.println(reutlt);
                        return;

                    }

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
