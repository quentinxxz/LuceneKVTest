/*
 * Copyright 2016 Alibaba.com All right reserved. This software is the
 * confidential and proprietary information of Alibaba.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Alibaba.com.
 */
package com.quentinxxz.lucene.kv;

import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.analysis.util.CharacterUtils.CharacterBuffer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

import com.google.common.primitives.Ints;


/**
 * 类DWeightLabelTokenizer.java的实现描述：payload存储，double类型的权重信息
 * @author xianzhong.xxz 2016年2月6日 下午2:04:31
 */
public class IntPayloadTokenizer extends Tokenizer {

    public IntPayloadTokenizer(Version matchVersion, Reader input){
        super(input);
        charUtils = CharacterUtils.getInstance(matchVersion);
    }

    public IntPayloadTokenizer(Version matchVersion, Reader input, char termSplitChar, char weightSplitChar){
        super(input);
        charUtils = CharacterUtils.getInstance(matchVersion);
        this.termSplitChar = termSplitChar;
        this.weightSplitChar = weightSplitChar;
    }

    private static final int        MAX_WORD_LEN    = 255;
    private static final int        IO_BUFFER_SIZE  = 4096;

    private final CharTermAttribute termAtt         = addAttribute(CharTermAttribute.class);
    private final PayloadAttribute  payload         = addAttribute(PayloadAttribute.class);

    private final CharacterUtils    charUtils;
    private final CharacterBuffer   ioBuffer        = CharacterUtils.newCharacterBuffer(IO_BUFFER_SIZE);
    private int                     offset          = 0,
                                            bufferIndex = -1, dataLen = 0;
    private char                    termSplitChar   = ' ';
    private char                    weightSplitChar = ':';
    private Set<String>             labels          = new HashSet<String>();

    protected boolean isTokenChar(int c) {
        return !(termSplitChar == c);
    }

    protected int normalize(int c) {
        return c;
    }

    @Override
    public final boolean incrementToken() throws IOException {
        clearAttributes();
        int length = 0;
        int start = -1; // this variable is always initialized
        char[] buffer = termAtt.buffer();
        while (true) {
            if (bufferIndex >= dataLen) {
                offset += dataLen;
                charUtils.fill(ioBuffer, input); // read supplementary char aware with CharacterUtils
                if (ioBuffer.getLength() == 0) {
                    dataLen = 0; // so next offset += dataLen won't decrement offset
                    if (length > 0) {
                        break;
                    } else {
                        return false;
                    }
                }
                dataLen = ioBuffer.getLength();
                bufferIndex = 0;
            }
            // use CharacterUtils here to support < 3.1 UTF-16 code unit behavior if the char based methods are gone
            final int c = charUtils.codePointAt(ioBuffer.getBuffer(), bufferIndex, ioBuffer.getLength());
            final int charCount = Character.charCount(c);
            bufferIndex += charCount;

            if (isTokenChar(c)) { // if it's a token char
                if (length == 0) { // start of token
                    assert start == -1;
                    start = offset + bufferIndex - charCount;
                } else if (length >= buffer.length - 1) { // check if a supplementary could run out of bounds
                    buffer = termAtt.resizeBuffer(2 + length); // make sure a supplementary fits in the buffer
                }
                length += Character.toChars(normalize(c), buffer, length); // buffer it, normalized
                if (length >= MAX_WORD_LEN) // buffer overflow! make sure to check for >= surrogate pair could break ==
                                            // test
                    break;
            } else if (length > 0) // at non-Letter w/ chars
                break; // return 'em
        }
        String mayTerm = new String(buffer, 0, length);
        int index = mayTerm.indexOf(weightSplitChar);
        String label = mayTerm;
        int actualLength = length;
        int actualWeight = 0; // 默认为0
        if (index != -1) {
            label = mayTerm.substring(0, index);
            actualLength = index;
            actualWeight = Integer.parseInt(mayTerm.substring(index + 1));
        }
        assert start != -1;

        if (labels.contains(label)) {
            return incrementToken();
        } else {
            labels.add(label);
        }

        termAtt.setLength(actualLength);

        payload.setPayload(new BytesRef(Ints.toByteArray(actualWeight)));
        return true;
    }

    @Override
    public final void end() throws IOException {
        super.end();
    }

    @Override
    public void reset() throws IOException {
        bufferIndex = 0;
        offset = 0;
        dataLen = 0;
        ioBuffer.reset(); // make sure to reset the IO buffer!!
    }

}
