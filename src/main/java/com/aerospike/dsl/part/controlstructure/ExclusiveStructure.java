package com.aerospike.dsl.part.controlstructure;

import com.aerospike.dsl.part.AbstractPart;
import com.aerospike.dsl.part.ExpressionContainer;
import lombok.Getter;

import java.util.List;

@Getter
public class ExclusiveStructure extends AbstractPart {

    private final List<ExpressionContainer> operands;

    public ExclusiveStructure(List<ExpressionContainer> operands) {
        super(PartType.EXCLUSIVE_STRUCTURE);
        this.operands = operands;
    }
}
