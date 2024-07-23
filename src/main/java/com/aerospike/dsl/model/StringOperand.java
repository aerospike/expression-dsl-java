package com.aerospike.dsl.model;

import lombok.Getter;

@Getter
public class StringOperand extends AbstractPart {

    private final String string;

    public StringOperand(String string) {
        super(PartType.STRING_OPERAND);
        this.string = string;
    }
}
