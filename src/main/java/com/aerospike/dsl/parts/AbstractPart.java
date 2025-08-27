package com.aerospike.dsl.parts;

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
    protected Filter filter;
    protected boolean isPlaceholder;

    protected AbstractPart(PartType partType) {
        this.partType = partType;
        this.exp = null;
    }

    public enum PartType {
        INT_OPERAND,
        FLOAT_OPERAND,
        BOOL_OPERAND,
        STRING_OPERAND,
        LIST_OPERAND,
        MAP_OPERAND,
        WITH_OPERAND,
        WITH_STRUCTURE,
        WHEN_STRUCTURE,
        EXCLUSIVE_STRUCTURE,
        AND_STRUCTURE,
        OR_STRUCTURE,
        BASE_PATH,
        BIN_PART,
        LIST_PART,
        MAP_PART,
        PATH_OPERAND,
        PATH_FUNCTION,
        METADATA_OPERAND,
        EXPRESSION_CONTAINER,
        VARIABLE_OPERAND,
        PLACEHOLDER_OPERAND
    }
}
