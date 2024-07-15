package com.aerospike.dsl.model;

import lombok.Getter;

@Getter
public class IntOperand extends AbstractPart {

    private final Long value;

    public IntOperand(Long value) {
        super(AbstractPart.Type.INT_OPERAND);
        this.value = value;
    }
}
