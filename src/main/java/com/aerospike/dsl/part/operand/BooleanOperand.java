package com.aerospike.dsl.part.operand;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.part.AbstractPart;
import lombok.Getter;

@Getter
public class BooleanOperand extends AbstractPart implements ParsedValueOperand {

    private final Boolean value;

    public BooleanOperand(Boolean value) {
        super(PartType.BOOL_OPERAND);
        this.value = value;
    }

    public Exp getExp() {
        return Exp.val(value);
    }
}
