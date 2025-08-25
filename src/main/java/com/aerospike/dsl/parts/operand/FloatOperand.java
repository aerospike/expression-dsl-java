package com.aerospike.dsl.parts.operand;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;

@Getter
public class FloatOperand extends AbstractPart implements ParsedValueOperand {

    // Keeping the boxed type for interface compatibility
    private final Double value;

    public FloatOperand(Double value) {
        super(PartType.FLOAT_OPERAND);
        this.value = value;
    }

    @Override
    public PartType getType() {
        return PartType.FLOAT_OPERAND;
    }

    @Override
    public Exp getExp() {
        return Exp.val(value);
    }
}
