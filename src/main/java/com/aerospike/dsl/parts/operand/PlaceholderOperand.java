package com.aerospike.dsl.parts.operand;

import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;
import lombok.Setter;

@Getter
public class PlaceholderOperand extends AbstractPart implements ParsedValueOperand {

    private final int index;
    @Setter
    private Object value;
    // A field to track if placeholder has been resolved
    private boolean isResolved = false;

    public PlaceholderOperand(int index) {
        super(PartType.PLACEHOLDER_OPERAND);
        this.index = index;
    }

    @Override
    public PartType getType() {
        return super.getPartType();
    }

    @Override
    public void setPartType(PartType type) {
        if (type == PartType.PLACEHOLDER_OPERAND) {
            throw new IllegalArgumentException("Cannot resolve to PLACEHOLDER_OPERAND");
        }
        super.setPartType(type);
        isResolved = true;
    }

    // Overriding all type-specific methods from the interface
    @Override
    public String getStringOperandValue() {
        // Use parent's partType instead of getType() for type checking
        if (!isResolved) {
            throw new IllegalStateException("Placeholder is not yet resolved");
        }
        if (super.partType != PartType.STRING_OPERAND) {
            throw new IllegalStateException("Not resolved to a STRING_OPERAND");
        }
        return (String) getValue();
    }

    public Long getIntOperandValue() {
        if (!isResolved) {
            throw new IllegalStateException("Placeholder is not yet resolved");
        }
        if (super.partType != PartType.INT_OPERAND) {
            throw new IllegalStateException("Not resolved to an INT_OPERAND");
        }
        return ((Number) getValue()).longValue();
    }
}
