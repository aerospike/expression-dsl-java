package com.aerospike.dsl.parts.operand;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;

@Getter
public class IntOperand extends AbstractPart implements ParsedValueOperand {

    // Keeping the boxed type for interface compatibility
    private final Long value;

    public IntOperand(Long value) {
        super(AbstractPart.PartType.INT_OPERAND);
        this.value = value;
    }

    @Override
    public Exp getExp() {
        return Exp.val(value);
    }
}
