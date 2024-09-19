package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class StringOperand extends AbstractPart implements ParsedOperand {

    private final String value;

    public StringOperand(String string) {
        super(PartType.STRING_OPERAND, Exp.val(string));
        this.value = string;
    }
}
