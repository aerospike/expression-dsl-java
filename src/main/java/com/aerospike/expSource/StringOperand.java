package com.aerospike.expSource;

public class StringOperand extends ExpSource {

    public StringOperand(String string) {
        super(Type.STRING_OPERAND);
        super.string = string;
    }
}
