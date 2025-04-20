package com.aerospike.dsl.exception;

public class NoApplicableFilterException extends RuntimeException {

    public NoApplicableFilterException(String description) {
        super(description);
    }
}
