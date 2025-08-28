package com.aerospike.dsl.parts.controlstructure;

import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class WhenStructure extends AbstractPart {

    private List<AbstractPart> operands;

    public WhenStructure(List<AbstractPart> operands) {
        super(PartType.WHEN_STRUCTURE);
        this.operands = operands;
    }
}
