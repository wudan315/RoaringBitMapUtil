package org.apache.cassandra.cql3.functions.types.serialize;


import org.apache.cassandra.exceptions.SerialisationException;

/**
 * A class that implements this interface is responsible for serialising an
 * object of class T to a byte array, and for deserialising it back again.
 * It must also be able to deal with serialising null values.
 * @author dan.wu
 */
public interface ToBytesSerialiser<T> extends Serialiser<T, byte[]> {

    /**
     * Handle an incoming null value and generate an appropriate {@code byte[]} representation.
     */
    byte[] EMPTY_BYTES = new byte[0];

    /**
     * Handle an incoming null value and generate an appropriate byte array representation.
     *
     * @return byte[] the serialised bytes
     */
    @Override
    default byte[] serialiseNull() {
        return EMPTY_BYTES;
    }

    /**
     * Serialise some object and returns the raw bytes of the serialised form.
     *
     * @param object the object to be serialised
     * @return byte[] the serialised bytes
     * @throws SerialisationException if the object fails to serialise
     */
    @Override
    byte[] serialise(final T object) throws SerialisationException, SerialisationException, SerialisationException, SerialisationException;

    /**
     * @param allBytes The bytes to be decoded into characters
     * @param offset   The index of the first byte to decode
     * @param length   The number of bytes to decode
     * @return T the deserialised object
     * @throws SerialisationException issues during deserialisation
     */
    default T deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        final byte[] selection = new byte[length];
        try {
            System.arraycopy(allBytes, offset, selection, 0, length);
        } catch (final NullPointerException e) {
            throw new SerialisationException(String.format("Deserialising with giving range caused ArrayIndexOutOfBoundsException. byte[].size:%d startPos:%d length:%d", allBytes.length, 0, length), e);
        }
        return deserialise(selection);
    }

    /**
     * Deserialise an array of bytes into the original object.
     *
     * @param bytes the bytes to deserialise
     * @return T the deserialised object
     * @throws SerialisationException if the object fails to deserialise
     * <p>
     * Note that this implementation is less efficient than using deserialise
     * with an offset and a length, but may still be used if necessary.
     * @see #deserialise(byte[], int, int)
     */
    @Override
    T deserialise(final byte[] bytes) throws SerialisationException, SerialisationException;

    /**
     * Handle an empty byte array and reconstruct an appropriate representation in T form.
     *
     * @return T the deserialised object
     * @throws SerialisationException if the object fails to deserialise
     */
    @Override
    T deserialiseEmpty() throws SerialisationException;

    /**
     * Indicates whether the serialisation process preserves the ordering of the T,
     * i.e. if x and y are objects of class T, and x is less than y, then this method should
     * return true if the serialised form of x is guaranteed to be less than the serialised form
     * of y (using the standard ordering of byte arrays).
     * If T is not Comparable then this test makes no sense and false should be returned.
     *
     * @return true if the serialisation will preserve the order of the T, otherwise false.
     */
    @Override
    boolean preservesObjectOrdering();
}
