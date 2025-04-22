package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class FloatOperand extends AbstractPart implements ParsedOperand {

    private final Double value;

    public FloatOperand(Double value) {
        super(PartType.FLOAT_OPERAND);
        this.value = value;
    }

    public Exp getExp() {
        return Exp.val(value);
    }
}
