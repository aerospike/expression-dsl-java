package com.aerospike.dsl.parts.operand;

import com.aerospike.dsl.PlaceholderValues;
import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;

@Getter
public class PlaceholderOperand extends AbstractPart {

    private final int index;

    public PlaceholderOperand(int index) {
        super(PartType.PLACEHOLDER_OPERAND);
        super.isPlaceholder = true;
        this.index = index;
    }

    /**
     * Resolve placeholder's value based on index in {@link PlaceholderValues} and create a corresponding operand using
     * {@link OperandFactory#createOperand(Object)}
     * @param values Values to be matched with placeholders by index
     * @return Created {@link AbstractPart} operand
     */
    public AbstractPart resolve(PlaceholderValues values) {
        Object resolvedValue = values.getValue(index);
        return OperandFactory.createOperand(resolvedValue);
    }
}
