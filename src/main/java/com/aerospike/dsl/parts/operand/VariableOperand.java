package com.aerospike.dsl.parts.operand;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;

@Getter
public class VariableOperand extends AbstractPart implements ParsedValueOperand {

    private final String value;

    public VariableOperand(String name) {
        super(PartType.VARIABLE_OPERAND);
        this.value = name;
    }

    public Exp getExp() {
        return Exp.var(value);
    }
}
