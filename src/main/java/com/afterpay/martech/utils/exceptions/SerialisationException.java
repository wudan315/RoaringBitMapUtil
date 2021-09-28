package com.afterpay.martech.utils.exceptions;

import java.io.IOException;

/**
 * @author dan.wu
 */
public class SerialisationException extends IOException {
    private static final long serialVersionUID = 1624476078972832393L;

    public SerialisationException(final String message) {
        super(message);
    }

    public SerialisationException(final String message, final Throwable e) {
        super(message, e);
    }
}
