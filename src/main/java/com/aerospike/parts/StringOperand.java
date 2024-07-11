package com.aerospike.parts;

import lombok.Getter;

@Getter
public class StringOperand extends AbstractPart {

    private final String string;

    public StringOperand(String string) {
        super(Type.STRING_OPERAND, null);
        this.string = string;
    }
}
