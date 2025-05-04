package com.aerospike.dsl.parts.path;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.parts.ExpressionContainer;
import lombok.Getter;

@Getter
public class BinPart extends ExpressionContainer {

    private final String binName;

    public BinPart(String binName) {
        super();
        this.binName = binName;
        this.partType = PartType.BIN_PART;
        this.expType = null; // Exp type unknown
    }

    public void updateExp(Exp.Type expType) {
        this.expType = expType;
    }

    public Exp getExp() {
        return Exp.bin(this.binName, expType);
    }
}
