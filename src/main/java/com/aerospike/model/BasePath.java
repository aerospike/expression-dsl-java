package com.aerospike.model;

import lombok.Getter;

import java.util.List;

@Getter
public class BasePath extends AbstractPart {

    private final BinPart binPart;
    private final List<AbstractPart> parts;

    public BasePath(BinPart binOperand, List<AbstractPart> parts) {
        super(Type.BASE_PATH);
        this.binPart = binOperand;
        this.parts = parts;
    }
}
