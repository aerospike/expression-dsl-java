package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;
import lombok.Setter;

@Getter
public class BinPart extends Expr {

    private final String binName;
    @Setter
    private Exp.Type expType;

    public BinPart(String binName) {
        super(constructExp(binName));
        this.binName = binName;
        this.setPartType(PartType.BIN_PART);
        this.expType = null;
    }

    // Bin is implicitly a boolean expression by itself
    private static Exp constructExp(String binName) {
        return Exp.boolBin(binName);
    }
}
