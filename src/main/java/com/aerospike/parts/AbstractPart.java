package com.aerospike.parts;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public abstract class AbstractPart {

    private final Type type;
    private final Exp exp;

    public AbstractPart(Type type, Exp exp) {
        this.type = type;
        this.exp = exp;
    }

    public enum Type {
        NUMBER_OPERAND,
        STRING_OPERAND,
        BIN_PART,
        PATH_OPERAND,
        BASE_PATH,
        LIST_PART,
        PATH_FUNCTION,
        METADATA_OPERAND,
        EXPR
    }
}
