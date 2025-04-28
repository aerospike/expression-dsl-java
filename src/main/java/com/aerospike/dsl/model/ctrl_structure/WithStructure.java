package com.aerospike.dsl.model.ctrl_structure;

import com.aerospike.dsl.model.AbstractPart;
import lombok.Getter;

import java.util.List;

@Getter
public class WithStructure extends AbstractPart {

    private final List<WithOperand> operands;

    public WithStructure(List<WithOperand> operands) {
        super(PartType.WITH_STRUCTURE);
        this.operands = operands;
    }
}
