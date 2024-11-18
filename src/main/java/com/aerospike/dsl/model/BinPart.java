package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class BinPart extends Expr {

    private final String binName;

    public BinPart(String binName) {
        super((Exp) null); // Exp unknown
        this.binName = binName;
        this.partType = PartType.BIN_PART;
        this.expType = null; // Exp type unknown
    }

    public void updateExp(Exp.Type expType) {
        this.expType = expType;
        // Update Expression of Abstract Part
        super.exp = Exp.bin(this.binName, expType);
    }
}
