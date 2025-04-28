package com.aerospike.dsl.part.controlstructure;

import com.aerospike.dsl.part.AbstractPart;
import com.aerospike.dsl.part.operand.WithOperand;
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
