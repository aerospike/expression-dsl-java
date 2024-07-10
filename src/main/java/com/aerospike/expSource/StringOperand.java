package com.aerospike.expSource;

import lombok.Getter;

@Getter
public class StringOperand extends AbstractPart {

    private final String string;

    public StringOperand(String string) {
        super(Type.STRING_OPERAND);
        this.string = string;
    }
}
