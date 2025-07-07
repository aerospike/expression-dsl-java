package com.aerospike.dsl.parts.path;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.parts.ExpressionContainer;
import lombok.Getter;
import lombok.Setter;

@Getter
public class BinPart extends ExpressionContainer {

    private final String binName;
    @Setter
    private boolean isTypeExplicitlySet;

    public BinPart(String binName) {
        super();
        this.binName = binName;
        this.partType = PartType.BIN_PART;
        this.expType = Exp.Type.INT; // Set INT by default
    }

    public void updateExp(Exp.Type expType) {
        this.expType = expType;
    }

    @Override
    public Exp getExp() {
        return Exp.bin(this.binName, expType);
    }
}
