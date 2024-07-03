package com.aerospike.expSource;

public class NumberOperand extends ExpSource {

    public NumberOperand(Long number) {
        super(Type.NUMBER_OPERAND);
        super.number = number;
    }
}
