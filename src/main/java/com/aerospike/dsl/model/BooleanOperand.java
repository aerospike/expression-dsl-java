package com.aerospike.dsl.model;

import lombok.Getter;

@Getter
public class BooleanOperand extends AbstractPart {

    private final Boolean value;

    public BooleanOperand(Boolean value) {
        super(PartType.BOOL_OPERAND);
        this.value = value;
    }
}
