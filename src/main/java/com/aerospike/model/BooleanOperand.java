package com.aerospike.model;

import lombok.Getter;

@Getter
public class BooleanOperand extends AbstractPart {

    private final Boolean value;

    public BooleanOperand(Boolean value) {
        super(Type.BOOL_OPERAND);
        this.value = value;
    }
}
