package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class BinPart extends Expr {

    private final String binName;
    private Exp.Type expType;

    public BinPart(String binName) {
        super(null); // Exp unknown
        this.binName = binName;
        this.setPartType(PartType.BIN_PART);
        this.expType = null; // Exp type unknown
    }

    public void updateExp(Exp.Type expType) {
        this.expType = expType;
        // Update Expression of Abstract Part
        super.setExp(Exp.bin(this.binName, expType));
    }
}
