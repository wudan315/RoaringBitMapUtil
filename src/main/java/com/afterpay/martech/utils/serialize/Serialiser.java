package com.afterpay.martech.utils.serialize;



import com.afterpay.martech.utils.exceptions.SerialisationException;

import java.io.Serializable;

/**
 * @author dan.wu
 */
public interface Serialiser<INPUT, OUTPUT> extends Serializable {

    /**
     * Handle an incoming null value and generate an appropriate OUTPUT representation.
     *
     * @return OUTPUT the serialised output
     */
    OUTPUT serialiseNull();

    /**
     * Check whether the serialiser can serialise a particular class.
     * <p>
     * //Warning: Due to erasure you may get false positives. e.g.
     * <pre>
     * {@code
     * Set<Foo> set = Sets.newHashSet();
     * boolean b = new Serialiser<Set<Bar>, byte[]>().canHandle(set.getClass());
     * b will incorrectly be true because of type erasure between Set<Foo> Set<Bar>
     * }
     * </pre>
     *
     * @param clazz the object class to serialise
     * @return boolean true if it can be handled
     */
    boolean canHandle(final Class clazz);

    /**
     * Serialise some object and returns the OUTPUT of the serialised form.
     *
     * @param object the object to be serialised
     * @return OUTPUT the serialised OUTPUT
     * @throws SerialisationException if the object fails to serialise
     */
    OUTPUT serialise(final INPUT object) throws SerialisationException, SerialisationException, SerialisationException;

    /**
     * Deserialise an OUTPUT into the original object.
     *
     * @param output the OUTPUT to deserialise
     * @return INPUT the deserialised object
     * @throws SerialisationException if the object fails to deserialise
     */
    INPUT deserialise(final OUTPUT output) throws SerialisationException;

    /**
     * Handle an empty OUTPUT and reconstruct an appropriate representation in Object form.
     *
     * @return INPUT the deserialised object
     * @throws SerialisationException if the object fails to deserialise
     */
    INPUT deserialiseEmpty() throws SerialisationException;

    /**
     * Indicates whether the serialisation process preserves the ordering of the INPUT,
     * i.e. if x and y are objects of class INPUT, and x is less than y, then this method should
     * return true if the serialised form of x is guaranteed to be less than the serialised form
     * of y (using the standard ordering of OUTPUT).
     * If INPUT is not Comparable then this test makes no sense and false should be returned.
     *
     * @return true if the serialisation will preserve the order of the INPUT, otherwise false.
     */
    boolean preservesObjectOrdering();

    /**
     * Indicates whether the serialisation process produces a predictable, consistent
     * OUTPUT, from a given INPUT, ie the same object should always serialise in the same way
     * for this to be true.
     *
     * @return true if serialisation is consistent for a given object, otherwise false.
     */
    boolean isConsistent();
}