package com.aerospike.dsl.model;

import lombok.Getter;

@Getter
public class FloatOperand extends AbstractPart {

    private final Double value;

    public FloatOperand(Double value) {
        super(PartType.FLOAT_OPERAND);
        this.value = value;
    }
}
