package com.aerospike.dsl.model;

import lombok.Getter;

import java.util.List;

@Getter
public class WhenStructure extends AbstractPart {

    private final List<AbstractPart> operands;

    public WhenStructure(List<AbstractPart> operands) {
        super(PartType.WHEN_STRUCTURE);
        this.operands = operands;
    }
}
