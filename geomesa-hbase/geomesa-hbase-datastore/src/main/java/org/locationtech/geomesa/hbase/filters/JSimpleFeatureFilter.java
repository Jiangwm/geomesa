/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.filters;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.features.interop.SerializationOptions;
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JSimpleFeatureFilter extends FilterBase {
    private String sftString;
    private SimpleFeatureType sft;

    private org.opengis.filter.Filter filter;
    private String filterString;

    private static Logger log = LoggerFactory.getLogger(JSimpleFeatureFilter.class);

    private static LoadingCache<String, SimpleFeatureType> sfts =
            CacheBuilder.newBuilder().build(new CacheLoader<String, SimpleFeatureType>() {
                @Override
                public SimpleFeatureType load(String s) throws Exception {
                    log.debug("Caching a new SFT: {}", s);
                    return SimpleFeatureTypes.createType("", s);
                }
            });

    private static GenericKeyedObjectPool<SimpleFeatureType, KryoFeatureSerializer> serializers =
            new GenericKeyedObjectPool<>(new BaseKeyedPoolableObjectFactory<SimpleFeatureType, KryoFeatureSerializer>() {
                @Override
                public KryoFeatureSerializer makeObject(SimpleFeatureType key) throws Exception {
                    log.debug("Getting access to a new KryoFeatureSerializer: {}", key);
                    return new KryoFeatureSerializer(key, SerializationOptions.withoutId());
                }
            });


    public JSimpleFeatureFilter(String sftString, String filterString) {
        log.debug("Instantiating new JSimpleFeatureFilter, sftString = {}, filterString = {}", sftString, filterString);
        this.sftString = sftString;
        sft = sfts.getUnchecked(sftString);

        this.filterString = filterString;
        if (filterString != null && filterString != "") {
            try {
                this.filter = ECQL.toFilter(this.filterString);
            } catch (CQLException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public JSimpleFeatureFilter(SimpleFeatureType sft, org.opengis.filter.Filter filter) {
        this.sft = sft;
        this.filter = filter;
        this.sftString = SimpleFeatureTypes.encodeType(sft, true);
        this.filterString = ECQL.toCQL(filter);
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        if (filter == null) {
            return ReturnCode.INCLUDE;
        } else {
            try {
                KryoFeatureSerializer serializer = serializers.borrowObject(this.sft);
                try {
                    byte[] valueBytes = v.getValueArray();
                    int valueOffset = v.getValueOffset();
                    int valueLength = v.getValueLength();
                    log.debug("valueBytes.length = {}, valueOffset = {}, valueLength = {}",
                            valueBytes.length, valueOffset, valueLength);
                    SimpleFeature sf = serializer.deserialize(valueBytes, valueOffset, valueLength);
                    log.debug("Evaluating filter against SimpleFeature");
                    if (filter.evaluate(sf)) {
                        return ReturnCode.INCLUDE;
                    } else {
                        return ReturnCode.SKIP;
                    }
                } finally {
                    serializers.returnObject(this.sft, serializer);
                }
            } catch(Exception e) {
                log.error("Exception thrown while scanning, skipping entry", e);
                return ReturnCode.SKIP;
            }
        }
    }

    @Override
    public Cell transformCell(Cell v) throws IOException {
        return super.transformCell(v);
    }

    // TODO: Add static method to compute byte array from SFT and Filter.
    @Override
    public byte[] toByteArray() throws IOException {
        return Bytes.add(getLengthArray(sftString), getLengthArray(filterString));
    }

    private byte[] getLengthArray(String s) {
        int len = getLen(s);
        if (len == 0) {
            return Bytes.toBytes(0);
        } else {
            return Bytes.add(Bytes.toBytes(len), s.getBytes());
        }
    }

    private int getLen(String s) {
        if (s != null) {
            return s.length();
        } else {
            return 0;
        }
    }

    public static org.apache.hadoop.hbase.filter.Filter parseFrom(final byte [] pbBytes) throws DeserializationException {
        int sftLen =  Bytes.readAsInt(pbBytes, 0, 4);
        String sftString = new String(Bytes.copy(pbBytes, 4, sftLen));

        int filterLen = Bytes.readAsInt(pbBytes, sftLen + 4, 4);
        String filterString = new String(Bytes.copy(pbBytes, sftLen + 8, filterLen));

        return new JSimpleFeatureFilter(sftString, filterString);
    }

}
