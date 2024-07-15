package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class BinPart extends Expr {

    private final String binName;

    public BinPart(String binName) {
        super(constructExp(binName));
        this.binName = binName;
        this.setType(Type.BIN_PART);
    }

    // Bin is implicitly a boolean expression by itself
    private static Exp constructExp(String binName) {
        return Exp.boolBin(binName);
    }
}
