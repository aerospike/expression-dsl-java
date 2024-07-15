package com.aerospike.model;

import lombok.Getter;

@Getter
public class BinPart extends AbstractPart {

    private final String binName;

    public BinPart(String binName) {
        super(Type.BIN_PART);
        this.binName = binName;
    }
}
