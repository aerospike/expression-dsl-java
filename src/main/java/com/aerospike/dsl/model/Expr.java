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

    public Expr(SIndexFilter filter) {
        super(PartType.EXPR, filter);
    }

    public Expr(AbstractPart left, AbstractPart right, ExprPartsOperation operationType) {
        super(PartType.EXPR);
        this.left = left;
        this.right = right;
        this.operationType = operationType;
    }

    public enum ExprPartsOperation {
        ADD,
        SUB,
        DIV,
        MUL
    }
}
