package com.aerospike.dsl.parts.operand;

import com.aerospike.dsl.parts.AbstractPart;

/**
 * This interface provides an abstraction for an operand that returns a single value to be used for constructing
 * the resulting filter (e.g., a String for StringOperand or a list of objects for ListOperand)
 */
public interface ParsedValueOperand {

    Object getValue();

    AbstractPart.PartType getType();

    // Default implementations for type-specific access
    default String getStringOperandValue() {
        if (getType() != AbstractPart.PartType.STRING_OPERAND) {
            throw new IllegalStateException("Not a STRING_OPERAND");
        }
        return (String) getValue();
    }

    default Long getIntOperandValue() {
        if (getType() != AbstractPart.PartType.INT_OPERAND) {
            throw new IllegalStateException("Not an INT_OPERAND");
        }
        return (Long) getValue();
    }
}
