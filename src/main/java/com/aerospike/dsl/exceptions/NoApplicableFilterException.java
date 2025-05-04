package com.aerospike.dsl.exceptions;

import com.aerospike.client.query.Filter;

/**
 * Indicates that no applicable {@link Filter} could be generated for a given DSL expression.
 *
 * <p>This exception is typically thrown when attempting to create a Filter for a DSL expression
 * but the structure or types of the expression do not match any supported filtering patterns
 * (e.g., comparing Strings using arithmetical operations, using OR-combined expression etc.).
 * It signifies that while the expression might be valid in a broader context, it cannot be represented with a Filter.
 */
public class NoApplicableFilterException extends RuntimeException {

    public NoApplicableFilterException(String description) {
        super(description);
    }
}
