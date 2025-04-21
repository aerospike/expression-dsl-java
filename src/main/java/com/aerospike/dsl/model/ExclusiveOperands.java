package com.aerospike.dsl.model;

import lombok.Getter;

import java.util.List;

@Getter
public class ExclusiveOperands extends AbstractPart {

    private final List<Expr> operands;

    public ExclusiveOperands(List<Expr> operands) {
        super(PartType.EXCLUSIVE_OPERANDS_LIST);
        this.operands = operands;
    }
}
