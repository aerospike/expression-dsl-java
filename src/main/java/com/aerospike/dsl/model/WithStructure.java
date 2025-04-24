package com.aerospike.dsl.model;

import lombok.Getter;

import java.util.List;

@Getter
public class WithStructure extends AbstractPart {

    private final List<WithOperand> operands;

    public WithStructure(List<WithOperand> operands) {
        super(PartType.WITH_STRUCTURE);
        this.operands = operands;
    }
}
