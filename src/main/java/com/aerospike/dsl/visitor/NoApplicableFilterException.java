package com.aerospike.dsl.visitor;

import com.aerospike.client.query.Filter;

/**
 * Indicates that no applicable {@link Filter} could be generated for a given DSL expression. For internal use.
 *
 * <p>This exception is typically thrown when attempting to create a Filter for a DSL expression
 * but the structure or types of the expression do not match any supported filtering patterns
 * (e.g., comparing Strings using arithmetical operations, using OR-combined expression etc.).
 * It signifies that while the expression might be valid in a broader context, it cannot be represented with a
 * secondary index Filter.
 */
class NoApplicableFilterException extends RuntimeException {

    NoApplicableFilterException(String description) {
        super(description);
    }
}
