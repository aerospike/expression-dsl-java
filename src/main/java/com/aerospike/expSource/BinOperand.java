package com.aerospike.expSource;

public class BinOperand extends ExpSource {

    public BinOperand(String binName) {
        super(Type.BIN_OPERAND);
        super.binName = binName;
    }
}
