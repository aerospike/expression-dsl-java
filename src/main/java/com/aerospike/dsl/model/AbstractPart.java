package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class AbstractPart {

    protected Exp.Type expType;

    protected PartType partType;
    protected Exp exp;

    protected AbstractPart(PartType partType) {
        this.partType = partType;
        this.exp = null;
    }

    protected AbstractPart(PartType partType, Exp exp) {
        this.partType = partType;
        this.exp = exp;
    }

    public enum PartType {
        INT_OPERAND,
        FLOAT_OPERAND,
        BOOL_OPERAND,
        STRING_OPERAND,
        LIST_OPERAND,
        MAP_OPERAND,
        BASE_PATH,
        BIN_PART,
        LIST_PART,
        MAP_PART,
        PATH_OPERAND,
        PATH_FUNCTION,
        METADATA_OPERAND,
        EXPR,
        VARIABLE_OPERAND
    }
}
