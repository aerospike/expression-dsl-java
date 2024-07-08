package com.aerospike.expSource;

import lombok.Getter;

@Getter
public class NumberOperand extends AbstractPart {

    private final Long number;

    public NumberOperand(Long number) {
        super(Type.NUMBER_OPERAND, null);
        this.number = number;
    }
}
