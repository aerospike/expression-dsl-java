package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class AbstractPart {

    @Setter
    private PartType partType;
    private final Exp exp;

    public AbstractPart(PartType partType) {
        this.partType = partType;
        this.exp = null;
    }

    public AbstractPart(PartType partType, Exp exp) {
        this.partType = partType;
        this.exp = exp;
    }

    public enum PartType {
        INT_OPERAND,
        FLOAT_OPERAND,
        BOOL_OPERAND,
        STRING_OPERAND,
        BASE_PATH,
        BIN_PART,
        LIST_PART,
        PATH_OPERAND,
        PATH_FUNCTION,
        METADATA_OPERAND,
        EXPR,
        VARIABLE_OPERAND
    }
}
