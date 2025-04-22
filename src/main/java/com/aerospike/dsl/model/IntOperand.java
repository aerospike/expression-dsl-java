package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class IntOperand extends AbstractPart implements ParsedOperand {

    private final Long value;

    public IntOperand(Long value) {
        super(AbstractPart.PartType.INT_OPERAND);
        this.value = value;
    }

    public Exp getExp() {
        return Exp.val(value);
    }
}
