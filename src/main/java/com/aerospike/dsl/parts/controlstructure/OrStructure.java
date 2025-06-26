package com.aerospike.dsl.parts.controlstructure;

import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.ExpressionContainer;
import lombok.Getter;

import java.util.List;

@Getter
public class OrStructure extends AbstractPart {

    private final List<ExpressionContainer> operands;

    public OrStructure(List<ExpressionContainer> operands) {
        super(PartType.OR_STRUCTURE);
        this.operands = operands;
    }
}
