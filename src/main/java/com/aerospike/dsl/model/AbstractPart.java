package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.annotation.Beta;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Beta
public abstract class AbstractPart {

    private PartType partType;
    private Exp exp;
    Exp.Type expType;

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
