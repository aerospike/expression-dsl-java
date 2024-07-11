package com.aerospike.parts;

import lombok.Getter;

import java.util.List;

@Getter
public class BasePath extends AbstractPart {

    private final BinPart binOperand;
    private final List<AbstractPart> parts;

    public BasePath(BinPart binOperand, List<AbstractPart> parts) {
        super(Type.BASE_PATH, null);
        this.binOperand = binOperand;
        this.parts = parts;
    }
}
