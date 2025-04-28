package com.aerospike.dsl.model.ctrl_structure;

import com.aerospike.dsl.model.AbstractPart;
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
