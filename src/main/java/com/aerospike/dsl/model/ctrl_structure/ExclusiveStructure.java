package com.aerospike.dsl.model.ctrl_structure;

import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.model.ExpressionContainer;
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
