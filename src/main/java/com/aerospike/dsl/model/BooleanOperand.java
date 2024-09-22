package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class BooleanOperand extends AbstractPart implements ParsedOperand {

    private final Boolean value;

    public BooleanOperand(Boolean value) {
        super(PartType.BOOL_OPERAND, Exp.val(value));
        this.value = value;
    }
}
