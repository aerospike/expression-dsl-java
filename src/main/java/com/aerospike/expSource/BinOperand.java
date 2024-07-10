package com.aerospike.expSource;

import lombok.Getter;

@Getter
public class BinOperand extends AbstractPart {

    private final String binName;

    public BinOperand(String binName) {
        super(Type.BIN_OPERAND);
        this.binName = binName;
    }
}
