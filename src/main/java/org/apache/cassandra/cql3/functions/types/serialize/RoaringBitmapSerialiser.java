package org.apache.cassandra.cql3.functions.types.serialize;

/*
 * Copyright 2017-2020 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.cassandra.cql3.functions.types.RoaringBitmapUtils;
import org.apache.cassandra.exceptions.SerialisationException;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import java.io.*;


/**
 * @author dan.wu
 */
public class RoaringBitmapSerialiser implements ToBytesSerialiser<MutableRoaringBitmap> {

    private static final long serialVersionUID = 3772387954385745791L;

    @Override
    public boolean canHandle(final Class clazz) {
        return MutableRoaringBitmap.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final MutableRoaringBitmap object) throws SerialisationException {
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(byteOut);
        try {
            object.serialize(out);
        } catch (final IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        return byteOut.toByteArray();
    }

    @Override
    public MutableRoaringBitmap deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        final MutableRoaringBitmap value = new MutableRoaringBitmap();
        final byte[] convertedBytes = RoaringBitmapUtils.upConvertSerialisedForm(allBytes, offset, length);
        final ByteArrayInputStream byteIn = new ByteArrayInputStream(convertedBytes);
        final DataInputStream in = new DataInputStream(byteIn);
        try {
            value.deserialize(in);
        } catch (final IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        return value;
    }

    @Override
    public MutableRoaringBitmap deserialise(final byte[] bytes) throws SerialisationException {
        return deserialise(bytes, 0, bytes.length);
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }

    @Override
    public boolean isConsistent() {
        return false;
    }

    @Override
    public MutableRoaringBitmap deserialiseEmpty() {
        return new MutableRoaringBitmap();
    }

    @Override
    public byte[] serialiseNull() {
        return new byte[0];
    }

    @Override
    public boolean equals(final Object obj) {
        return this == obj || obj != null && this.getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return RoaringBitmapSerialiser.class.getName().hashCode();
    }
}