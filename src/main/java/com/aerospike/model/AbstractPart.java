package com.aerospike.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class AbstractPart {

    @Setter
    private Type type;
    private final Exp exp;

    public AbstractPart(Type type) {
        this.type = type;
        this.exp = null;
    }

    public AbstractPart(Type type, Exp exp) {
        this.type = type;
        this.exp = exp;
    }

    public enum Type {
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
        EXPR
    }
}
