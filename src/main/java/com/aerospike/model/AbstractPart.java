package com.aerospike.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public abstract class AbstractPart {

    private final Type type;
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
        STRING_OPERAND,
        BIN_OPERAND,
        METADATA_OPERAND,
        EXPR
    }
}
