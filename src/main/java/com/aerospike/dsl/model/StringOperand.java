package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class StringOperand extends AbstractPart {

    private final String string;

    public StringOperand(String string) {
        super(Type.STRING_OPERAND, Exp.val(string));
        this.string = string;
    }
}
