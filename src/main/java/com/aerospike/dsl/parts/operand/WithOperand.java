package com.aerospike.dsl.parts.operand;

import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;
import lombok.Setter;

@Getter
public class WithOperand extends AbstractPart {

    private final String string;
    @Setter
    private AbstractPart part;
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
