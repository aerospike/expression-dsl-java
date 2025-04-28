package com.aerospike.dsl.model;

import lombok.Getter;

@Getter
public class ExpressionContainer extends AbstractPart {

    protected AbstractPart left;
    protected AbstractPart right;
    private ExprPartsOperation operationType;
    private final boolean isUnary;

    public ExpressionContainer() {
        super(PartType.EXPRESSION_CONTAINER);
        this.isUnary = false;
    }

    public ExpressionContainer(AbstractPart left, AbstractPart right, ExprPartsOperation operationType) {
        super(PartType.EXPRESSION_CONTAINER);
        this.left = left;
        this.right = right;
        this.operationType = operationType;
        this.isUnary = false;
    }

    public ExpressionContainer(AbstractPart singleOperand, ExprPartsOperation operationType) {
        super(PartType.EXPRESSION_CONTAINER);
        this.left = singleOperand;
        this.operationType = operationType;
        this.isUnary = true;
    }

    public enum ExprPartsOperation {
        ADD,
        SUB,
        DIV,
        MUL,
        MOD,
        INT_XOR,
        INT_NOT, // unary
        INT_AND,
        INT_OR,
        L_SHIFT,
        R_SHIFT,
        AND,
        OR,
        NOT, // unary
        EQ,
        NOTEQ,
        GT,
        GTEQ,
        LT,
        LTEQ,
        WITH_STRUCTURE, // unary
        WHEN_STRUCTURE, // unary
        EXCLUSIVE_STRUCTURE // unary
    }
}
