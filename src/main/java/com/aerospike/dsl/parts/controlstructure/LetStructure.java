package com.aerospike.dsl.parts.controlstructure;

import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.operand.LetOperand;
import lombok.Getter;

import java.util.List;

@Getter
public class LetStructure extends AbstractPart {

    private final List<LetOperand> operands;

    public LetStructure(List<LetOperand> operands) {
        super(PartType.LET_STRUCTURE);
        this.operands = operands;
    }
}
