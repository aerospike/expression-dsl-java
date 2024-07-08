package com.aerospike.expSource;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class Expr extends AbstractPart {

    public Expr(Exp exp) {
        super(Type.EXPR, exp);
    }
}
