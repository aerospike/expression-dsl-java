package com.aerospike.dsl.model;

import lombok.Getter;

import java.util.List;

@Getter
public class WhenOperands extends AbstractPart {

    private final List<AbstractPart> operands;

    public WhenOperands(List<AbstractPart> operands) {
        super(PartType.WHEN_OPERANDS_LIST);
        this.operands = operands;
    }
}
