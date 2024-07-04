package com.aerospike.expSource;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public abstract class ExpSource {

    private final Type type;
    String binName;
    Long number;
    String string;
    Exp exp;
    private String context;

    public ExpSource(Type type) {
        this.type = type;
    }

    public enum Type {
        NUMBER_OPERAND,
        STRING_OPERAND,
        BIN_OPERAND,
        METADATA_OPERAND,
        EXPR
    }
}
