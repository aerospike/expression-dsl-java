package com.aerospike.dsl.model.ctrl_structure;

import com.aerospike.dsl.model.AbstractPart;
import lombok.Getter;

@Getter
public class WithOperand extends AbstractPart {

    private final String string;
    private final AbstractPart part;
    private final boolean isLastPart;

    public WithOperand(AbstractPart part, String string) {
        super(PartType.WITH_OPERAND);
        this.string = string;
        this.part = part;
        this.isLastPart = false;
    }

    public WithOperand(AbstractPart part, boolean isLastPart) {
        super(PartType.WITH_OPERAND);
        this.string = null;
        this.part = part;
        this.isLastPart = isLastPart;
    }
}
