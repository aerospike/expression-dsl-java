package com.aerospike.dsl.model.simple;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.model.ParsedOperand;
import lombok.Getter;

@Getter
public class BooleanOperand extends AbstractPart implements ParsedOperand {

    private final Boolean value;

    public BooleanOperand(Boolean value) {
        super(PartType.BOOL_OPERAND);
        this.value = value;
    }

    public Exp getExp() {
        return Exp.val(value);
    }
}
