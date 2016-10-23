package com.quentinxxz.lucene.kv;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;

import com.google.common.primitives.Ints;

/**
 * 类LucenePayloadTest.java的实现描述：use lucene payload as value store
 * 
 * @author quentinxxz 2016年10月23日 下午3:31:55
 */
public class LucenePayloadTest {

    protected static class KeyField extends Field {

        public static final FieldType TYPE_NOT_STORED = new FieldType();

        static {
            TYPE_NOT_STORED.setIndexed(true);
            TYPE_NOT_STORED.setTokenized(true);
            // TYPE_NOT_STORED.setStoreTermVectors(true);
            TYPE_NOT_STORED.freeze();
        }

        public KeyField(String name, String value){
            super(name, new IntPayloadTokenizer(Version.LUCENE_45, new StringReader(value)), TYPE_NOT_STORED);
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
                    return LucenePayloadTest.getRandomString(20);
                }

            }).limit(2000000).forEach(key -> {
                keys.add(key);
                try {
                    writer.addDocument(LucenePayloadTest.getDocument(key, index.getAndIncrement()));
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
        IndexReader indexReader = DirectoryReader.open(new MMapDirectory(indexPath));

        // docValuesList = new ArrayList<NumericDocValues>();
        termsEnumList = new ArrayList<TermsEnum>();// 事先初始化termsEnumList，会有多线程问题，当多线程查询时，请用ThreadLocal封装
        for (AtomicReaderContext context : indexReader.leaves()) {
            // docValuesList.add(context.reader().getNumericDocValues("value"));
            termsEnumList.add(context.reader().terms("key").iterator(null));
        }

        // mmap方式查询
        for (int i = 0; i < 10; i++) {
            start = System.currentTimeMillis();
            keys.stream().limit(1000000).forEachOrdered(key -> {

                Term term = new Term("key", key);

                try {
                    for (int l = 0; l < termsEnumList.size(); l++) {

                        TermsEnum termsEnum = termsEnumList.get(l);
                        // TermsEnum termsEnum = ctx.reader().terms(term.field()).iterator(null);
                        if (termsEnum.seekExact(term.bytes()) == false) continue;
                        DocsAndPositionsEnum docsAndPositionsEnum = termsEnum.docsAndPositions(null, null,
                                                                                               DocsAndPositionsEnum.FLAG_PAYLOADS);
                        docsAndPositionsEnum.nextPosition();
                        int result = Ints.fromByteArray(docsAndPositionsEnum.getPayload().bytes);
                        // System.out.println(result);
                        return; // found

                    }
                    System.out.println("not found");

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
        doc.add(new KeyField("key", key + ":" + value));
        return doc;
    }

}
