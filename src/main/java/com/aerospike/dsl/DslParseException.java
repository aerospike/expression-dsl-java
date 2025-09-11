package com.aerospike.dsl;

/**
 * Represents a general processing exception that can occur during DSL expression parsing.
 * It is typically not expected to be caught by the caller, but rather indicates a potentially
 * unrecoverable issue like invalid input, failing validation or unsupported functionality.
 */
public class DslParseException extends RuntimeException {

    public DslParseException(String description) {
        super(description);
    }

    public DslParseException(String description, Throwable cause) {
        super(description, cause);
    }
}
