package com.quentinxxz.lucene.kv;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

import com.google.common.primitives.Ints;

/**
 * 类LuceneFSATest.java的实现描述：lucene FSA test
 * 
 * @author quentinxxz 2016年10月23日 下午3:30:51
 */
public class LuceneFSATest {

    public static void main(String args[]) throws IOException {
        List<String> keys = new ArrayList<String>();

        File indexPath = new File("/tmp/fsaLuceune");

        AtomicInteger index = new AtomicInteger(0);

        ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
        Builder<BytesRef> builder = new Builder<BytesRef>(FST.INPUT_TYPE.BYTE4, outputs);
        final IntsRef scratchIntsRef = new IntsRef();
        BytesRef output = new BytesRef(4);

        // 生成并插入500w条数据
        Stream.generate(new Supplier<String>() {

            @Override
            public String get() {
                return LuceneFSATest.getRandomString(20);
            }

        }).limit(1000000).sorted().forEach(key -> {
            keys.add(key);
            NumericUtils.intToPrefixCodedBytes(key.length(), 0, output);
            try {
                builder.add(Util.toUTF32(key, scratchIntsRef), new BytesRef(Ints.toByteArray(index.get())));
            } catch (Exception e) {
            }

            if (index.getAndIncrement() % 100000 == 0) System.out.println(index + " records has benn inserted");

        });
        FST<BytesRef> buildFst = builder.finish();

        File fstFile = new File(indexPath, "fst.bin");
        File fstDir = fstFile.getParentFile();
        if (!fstDir.exists()) {
            fstDir.mkdirs();
        }
        buildFst.save(fstFile);

        // 性能测试，10w次查询用时
        long start;
        MMapDirectory directory = new MMapDirectory(indexPath);
        IndexInput in = directory.openInput("fst.bin", null);
        FST<BytesRef> fst = new FST<BytesRef>(in, ByteSequenceOutputs.getSingleton());

        for (int i = 0; i < 5; i++) {
            start = System.currentTimeMillis();
            keys.stream().limit(1000000).forEachOrdered(key -> {

                try {
                    BytesRef value = Util.get(fst, new BytesRef(key));

                    if (value == null) {
                        System.out.println("not found");
                        return;
                    }
                    int reuslt = Ints.fromByteArray(value.bytes);
                    // System.out.println(result);

                } catch (IOException e) {
                    throw new RuntimeException(e);
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

}
