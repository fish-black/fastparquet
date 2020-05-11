package com.fishblack.fastparquet.common;

import java.io.IOException;

public class UnsupportedDataTypeException extends IOException {
    /**
     * Constructs an UnsupportedDataTypeException with no detail
     * message.
     */
    public UnsupportedDataTypeException() {
        super();
    }

    /**
     * Constructs an UnsupportedDataTypeException with the specified
     * message.
     *
     * @param message The detail message.
     */
    public UnsupportedDataTypeException(String message) {
        super(message);
    }
}
