package com.aerospike.dsl.parts.operand;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;

import static com.aerospike.dsl.parts.AbstractPart.PartType.BOOL_OPERAND;

@Getter
public class BooleanOperand extends AbstractPart implements ParsedValueOperand {

    // Keeping the boxed type for interface compatibility
    private final Boolean value;

    public BooleanOperand(Boolean value) {
        // Setting parent type
        super(BOOL_OPERAND);
        this.value = value;
    }

    @Override
    public PartType getType() {
        return BOOL_OPERAND;
    }

    @Override
    public Exp getExp() {
        return Exp.val(value);
    }
}
