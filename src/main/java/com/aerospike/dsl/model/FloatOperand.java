package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class FloatOperand extends AbstractPart {

    private final Double value;

    public FloatOperand(Double value) {
        super(PartType.FLOAT_OPERAND, Exp.val(value));
        this.value = value;
    }
}
