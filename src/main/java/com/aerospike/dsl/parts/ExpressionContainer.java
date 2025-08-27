package com.aerospike.dsl.parts;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
public class ExpressionContainer extends AbstractPart {

    @Setter
    protected AbstractPart left;
    @Setter
    protected AbstractPart right;
    private final boolean isUnary;
    private final ExprPartsOperation operationType;
    @Setter()
    @Accessors(fluent = true)
    private boolean hasSecondaryIndexFilter;
    @Setter()
    @Accessors(fluent = true)
    private boolean isExclFromSecondaryIndexFilter;

    public ExpressionContainer() {
        super(PartType.EXPRESSION_CONTAINER);
        this.isUnary = false;
        this.left = null;
        this.right = null;
        this.operationType = null;
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
        this.right = null;
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
        EXCLUSIVE_STRUCTURE, // unary
        AND_STRUCTURE,
        OR_STRUCTURE
    }
}
