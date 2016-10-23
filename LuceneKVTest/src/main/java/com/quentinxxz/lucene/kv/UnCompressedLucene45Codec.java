/*
 * Copyright 2016 Alibaba.com All right reserved. This software is the
 * confidential and proprietary information of Alibaba.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Alibaba.com.
 */
package com.quentinxxz.lucene.kv;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40LiveDocsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40SegmentInfoFormat;
import org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42FieldInfosFormat;
import org.apache.lucene.codecs.lucene42.Lucene42NormsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42TermVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/**
 * 类UnCompressedLucene45Codec.java的实现描述：uncompressed store lucene45Codec
 * 
 * @author quentinxxz 2016年10月23日 下午3:34:20
 */
public class UnCompressedLucene45Codec extends Codec {

    // private final StoredFieldsFormat fieldsFormat = new Lucene41StoredFieldsFormat();
    private final StoredFieldsFormat fieldsFormat     = new Lucene40StoredFieldsFormat();
    private final TermVectorsFormat  vectorsFormat    = new Lucene42TermVectorsFormat();
    private final FieldInfosFormat   fieldInfosFormat = new Lucene42FieldInfosFormat();
    private final SegmentInfoFormat  infosFormat      = new Lucene40SegmentInfoFormat();
    private final LiveDocsFormat     liveDocsFormat   = new Lucene40LiveDocsFormat();

    private final PostingsFormat     postingsFormat   = new PerFieldPostingsFormat() {

                                                          @Override
                                                          public PostingsFormat getPostingsFormatForField(String field) {
                                                              return UnCompressedLucene45Codec.this.getPostingsFormatForField(field);
                                                          }
                                                      };

    private final DocValuesFormat    docValuesFormat  = new PerFieldDocValuesFormat() {

                                                          @Override
                                                          public DocValuesFormat getDocValuesFormatForField(String field) {
                                                              return UnCompressedLucene45Codec.this.getDocValuesFormatForField(field);
                                                          }
                                                      };

    /** Sole constructor. */
    public UnCompressedLucene45Codec(){
        super("MyLucene45");
    }

    @Override
    public final StoredFieldsFormat storedFieldsFormat() {
        return fieldsFormat;
    }

    @Override
    public final TermVectorsFormat termVectorsFormat() {
        return vectorsFormat;
    }

    @Override
    public final PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public final SegmentInfoFormat segmentInfoFormat() {
        return infosFormat;
    }

    @Override
    public final LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }

    /**
     * Returns the postings format that should be used for writing new segments of <code>field</code>. The default
     * implementation always returns "Lucene41"
     */
    public PostingsFormat getPostingsFormatForField(String field) {
        return defaultFormat;
    }

    /**
     * Returns the docvalues format that should be used for writing new segments of <code>field</code>. The default
     * implementation always returns "Lucene45"
     */
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return defaultDVFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    private final PostingsFormat  defaultFormat   = PostingsFormat.forName("Lucene41");
    private final DocValuesFormat defaultDVFormat = DocValuesFormat.forName("Lucene45");

    private final NormsFormat     normsFormat     = new Lucene42NormsFormat();

    @Override
    public final NormsFormat normsFormat() {
        return normsFormat;
    }
}
