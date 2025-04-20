package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class Expr extends AbstractPart {

    protected AbstractPart left;
    protected AbstractPart right;
    private ExprPartsOperation operationType;

    public Expr(Exp exp) {
        super(PartType.EXPR, exp);
    }

    public Expr(AbstractPart left, AbstractPart right, ExprPartsOperation operationType) {
        super(PartType.EXPR);
        this.left = left;
        this.right = right;
        this.operationType = operationType;
    }

    public Expr(AbstractPart singleOperand, ExprPartsOperation operationType) {
        super(PartType.EXPR);
        this.left = singleOperand; // TODO: unary operator
        this.operationType = operationType;
    }

    public enum ExprPartsOperation {
        ADD,
        SUB,
        DIV,
        MUL,
        MOD,
        INT_XOR,
        INT_NOT,
        INT_AND,
        INT_OR,
        L_SHIFT,
        R_SHIFT,
        AND,
        OR,
        EQ,
        NOTEQ,
        GT,
        GTEQ,
        LT,
        LTEQ,
        WITH,
        WHEN // TODO
    }
}
