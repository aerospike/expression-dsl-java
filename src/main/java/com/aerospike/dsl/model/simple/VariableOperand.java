package com.aerospike.dsl.model.simple;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.AbstractPart;
import lombok.Getter;

@Getter
public class VariableOperand extends AbstractPart {

    private final String name;

    public VariableOperand(String name) {
        super(PartType.VARIABLE_OPERAND);
        this.name = name;
    }

    public Exp getExp() {
        return Exp.var(name);
    }
}
