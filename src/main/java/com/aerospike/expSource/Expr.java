package com.aerospike.expSource;

import com.aerospike.client.exp.Exp;

public class Expr extends ExpSource {

    public Expr(Exp exp) {
        super(Type.EXPR);
        super.exp = exp;
    }
}
