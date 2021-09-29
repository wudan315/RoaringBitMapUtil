package org.apache.cassandra.cql3.functions.types;

import org.apache.cassandra.cql3.functions.types.serialize.RoaringBitmapSerialiser;
import org.apache.cassandra.exceptions.SerialisationException;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;

/**
 * Contains a method for converting version 0.1.5 serialised RoaringBitmaps into
 * version 0.4.0-0.6.35 compatible forms.
 *
 * @author dan.wu
 */
public final class RoaringBitmapUtils {
    private static final int BITMAP_CONTAINER_SIZE = (1 << 16) / 8;
    private static final int MAX_ARRAY_CONTAINER_SIZE = 4096;
    private static final short VERSION_ZERO_ONE_FIVE_TO_ZERO_THREE_SEVEN_SERIAL_COOKIE = 12345;
    private static final short VERSION_ZERO_FOUR_ZERO_TO_SIX_THRIRTY_FIVE_NO_RUNCONTAINER_COOKIE = 12346;
    private static final short VERSION_ZERO_FIVE_ZERO_TO_SIX_THIRTY_FIVE_COOKIE = 12347;
    private static final byte[] VERSION_ZERO_FOUR_ZERO_TO_SIX_THIRTY_FIVE_NO_RUNCONTAINER_COOKIE_BYTES = ByteBuffer.allocate(4).putInt(Integer.reverseBytes(VERSION_ZERO_FOUR_ZERO_TO_SIX_THRIRTY_FIVE_NO_RUNCONTAINER_COOKIE)).array();
    private static final RoaringBitmapSerialiser SERIALISER = new RoaringBitmapSerialiser();

    private RoaringBitmapUtils() {

    }

    public static byte[] upConvertSerialisedForm(final byte[] serialisedBitmap, final int offset, final int length) throws SerialisationException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialisedBitmap, offset, length))) {
            int cookie;
            try {
                cookie = Integer.reverseBytes(input.readInt());
            } catch (final IOException e) {
                throw new SerialisationException("I failed to read the bitmap version cookie", e);
            }

            if (cookie == VERSION_ZERO_ONE_FIVE_TO_ZERO_THREE_SEVEN_SERIAL_COOKIE) {
                try {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
                    DataOutputStream out = new DataOutputStream(baos);
                    out.write(VERSION_ZERO_FOUR_ZERO_TO_SIX_THIRTY_FIVE_NO_RUNCONTAINER_COOKIE_BYTES);
                    int sizeInt = input.readInt();
                    int size = Integer.reverseBytes(sizeInt);

                    //Doesn't need to be reversed (already read as reversed)
                    out.writeInt(sizeInt);
                    int startOffSet = 4 + 4 + 4 * size + 4 * size;

                    //Need to extract the cardinalities to calculate the offsets
                    //Bitmap containers are a fixed size (BITMAP_CONTAINER_SIZE) and are used when the cardinality is greater than 4096
                    //Array containers are variable size (cardinality * 2)
                    int[] cardinalities = new int[size];
                    for (int i = 0; i < size; ++i) {

                        //Read and write the key (Don't need to use this just copy it across)
                        out.writeShort(input.readShort());

                        //Get the cardinality and store it for calculating offsets
                        short cardShort = input.readShort();
                        cardinalities[i] = 1 + (0xFFFF & Short.reverseBytes(cardShort));

                        //Doesn't need reversing (already read as reversed)
                        out.writeShort(cardShort);
                    }
                    int currentOffSet = startOffSet;
                    for (int i = 0; i < size; ++i) {
                        out.writeInt(Integer.reverseBytes(currentOffSet));
                        if (cardinalities[i] > MAX_ARRAY_CONTAINER_SIZE) {

                            //It's a bitmap container
                            currentOffSet += BITMAP_CONTAINER_SIZE;
                        } else {

                            //It's an array container
                            currentOffSet += (cardinalities[i] * 2);
                        }
                    }
                    int expectedNumContainerBytes = currentOffSet - startOffSet;

                    //Write out all the container data
                    int numContainerBytes = 0;
                    int b;
                    while ((b = input.read()) != -1) {
                        out.write(b);
                        ++numContainerBytes;
                    }
                    out.flush();
                    if (numContainerBytes != expectedNumContainerBytes) {
                        throw new SerialisationException("I failed to convert roaring bitmap from pre 0.4.0 version");
                    }
                    return baos.toByteArray();
                } catch (final SerialisationException e) {
                    throw e;
                } catch (final IOException e) {
                    throw new SerialisationException("IOException: I failed to convert roaring bitmap from pre 0.4.0 version", e);
                }
            } else if (cookie == VERSION_ZERO_FOUR_ZERO_TO_SIX_THRIRTY_FIVE_NO_RUNCONTAINER_COOKIE || (cookie & 0xFFFF) == VERSION_ZERO_FIVE_ZERO_TO_SIX_THIRTY_FIVE_COOKIE) {
                byte[] dest = new byte[length];
                System.arraycopy(serialisedBitmap, offset, dest, 0, length);
                return dest;
            } else {
                throw new SerialisationException("I failed to find a known roaring bitmap cookie (cookie = " + cookie + ")");
            }
        } catch (final IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    public static Long countMRB(List<MutableRoaringBitmap> mrbs) {
        if (null == mrbs || mrbs.size() == 0) {
            return 0L;
        }
        MutableRoaringBitmap mrb0 = mrbs.get(0);
        for (int i = 1; i < mrbs.size(); i++) {
            mrb0.or(mrbs.get(i));
        }
        return mrb0.getLongCardinality();
    }

    public static Long countMRB(String str) {
        byte[] byteStr = Base64.getDecoder().decode(str);
        MutableRoaringBitmap mrb;
        try {
            mrb = SERIALISER.deserialise(byteStr);
        } catch (SerialisationException e) {
            System.out.println("SerialisationException" + e);
            return null;
        }

        return mrb.getLongCardinality();
    }

    public static String getDistinctStr(String res, String distinStr) {
        MutableRoaringBitmap origin = new MutableRoaringBitmap();
        if (res != null) {
            origin = getRMB(res);
        }
        MutableRoaringBitmap mrb = getRMB(distinStr);
        origin.or(mrb);
        res = generateMRBStr(origin);
        return res;
    }


    public static String generateMRBStr(List<Long> ids) {
        int[] offsets = ids.stream()
                .mapToInt(Long::intValue)
                .toArray();
        MutableRoaringBitmap mrb = MutableRoaringBitmap.bitmapOf(offsets);
        mrb.runOptimize();
        byte[] bytes = new byte[0];
        try {
            bytes = SERIALISER.serialise(mrb);
        } catch (SerialisationException e) {
            e.printStackTrace();
        }
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static String generateMRBStr(MutableRoaringBitmap mrb) {
        mrb.runOptimize();
        byte[] bytes = new byte[0];
        try {
            bytes = SERIALISER.serialise(mrb);
        } catch (SerialisationException e) {
            e.printStackTrace();
        }
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static MutableRoaringBitmap getRMB(String str) {

        byte[] byteStr = Base64.getDecoder().decode(str);
        try {
            return SERIALISER.deserialise(byteStr);
        } catch (SerialisationException e) {
            System.out.println("SerialisationException" + e);
        }
        return null;
    }
}
