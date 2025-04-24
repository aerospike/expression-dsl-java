package com.aerospike.dsl.model;

import lombok.Getter;

import java.util.List;

@Getter
public class ExclusiveStructure extends AbstractPart {

    private final List<Expr> operands;

    public ExclusiveStructure(List<Expr> operands) {
        super(PartType.EXCLUSIVE_STRUCTURE);
        this.operands = operands;
    }
}
