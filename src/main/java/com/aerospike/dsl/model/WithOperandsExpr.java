package com.aerospike.dsl.model;

import lombok.Getter;

import java.util.List;

@Getter
public class WithOperandsExpr extends AbstractPart {

    private final List<WithOperand> operands;

    public WithOperandsExpr(List<WithOperand> operands) {
        super(PartType.WITH_OPERAND_EXPR);
        this.operands = operands;
    }
}
