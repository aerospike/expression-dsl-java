package com.aerospike.dsl.model;

import lombok.Getter;

import java.util.List;

@Getter
public class WithOperands extends AbstractPart {

    private final List<WithOperand> operands;

    public WithOperands(List<WithOperand> operands) {
        super(PartType.WITH_OPERANDS_LIST);
        this.operands = operands;
    }
}
