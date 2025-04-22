package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class AbstractPart {

    protected Exp.Type expType;
    protected PartType partType;
    protected Exp exp;
    protected Filter sIndexFilter;

    protected AbstractPart(PartType partType) {
        this.partType = partType;
        this.exp = null;
    }

    protected AbstractPart(PartType partType, Filter filter) {
        this.partType = partType;
        this.sIndexFilter = filter;
    }

    public enum PartType {
        INT_OPERAND,
        FLOAT_OPERAND,
        BOOL_OPERAND,
        STRING_OPERAND,
        LIST_OPERAND,
        MAP_OPERAND,
        WITH_OPERAND,
        WITH_OPERANDS_LIST,
        WHEN_OPERANDS_LIST,
        EXCLUSIVE_OPERANDS_LIST,
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
