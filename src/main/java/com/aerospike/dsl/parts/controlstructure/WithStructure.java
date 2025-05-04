package com.aerospike.dsl.parts.controlstructure;

import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.operand.WithOperand;
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
