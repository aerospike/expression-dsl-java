package com.aerospike.expSource;

import lombok.Getter;

@Getter
public class FloatOperand extends AbstractPart {

    private final Double value;

    public FloatOperand(Double value) {
        super(Type.FLOAT_OPERAND, null);
        this.value = value;
    }
}
