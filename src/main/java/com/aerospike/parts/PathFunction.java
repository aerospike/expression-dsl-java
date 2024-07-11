package com.aerospike.parts;

import lombok.Getter;

@Getter
public class PathFunction extends AbstractPart {

    private final PATH_FUNCTION_TYPE pathFunctionType;
    private final RETURN_PARAM returnParam;
    private final TYPE_PARAM typeParam;

    public PathFunction(PATH_FUNCTION_TYPE type, RETURN_PARAM returnParam, TYPE_PARAM typeParam) {
        super(Type.PATH_FUNCTION, null);
        this.pathFunctionType = type;
        this.returnParam = returnParam;
        this.typeParam = typeParam;
    }

    public enum TYPE_PARAM {
        INT,
        STR,
        HLL,
        BLOB,
        FLOAT,
        BOOL,
        LIST,
        MAP,
        GEO
    }

    public enum RETURN_PARAM {
        VALUE,
        COUNT,
        NONE
    }

    public enum PATH_FUNCTION_TYPE {
        GET,
        COUNT,
        SIZE
    }
}
