package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class VariableOperand extends AbstractPart {

    private final String name;

    public VariableOperand(String name) {
        super(PartType.VARIABLE_OPERAND, Exp.var(name));
        this.name = name;
    }
}
