package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class IntOperand extends AbstractPart {

    private final Long value;

    public IntOperand(Long value) {
        super(AbstractPart.PartType.INT_OPERAND, Exp.val(value));
        this.value = value;
    }
}
