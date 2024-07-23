package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class Expr extends AbstractPart {

    public Expr(Exp exp) {
        super(PartType.EXPR, exp);
    }
}
