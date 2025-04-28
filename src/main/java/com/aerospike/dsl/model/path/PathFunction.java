package com.aerospike.dsl.model.path;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.AbstractPart;
import lombok.Getter;

@Getter
public class PathFunction extends AbstractPart {

    private final PathFunctionType pathFunctionType;
    private final ReturnParam returnParam;
    private final Exp.Type binType;

    public PathFunction(PathFunctionType pathFunctionType, ReturnParam returnParam, Exp.Type binType) {
        super(PartType.PATH_FUNCTION);
        this.pathFunctionType = pathFunctionType;
        this.returnParam = returnParam;
        this.binType = binType;
    }

    public static Exp.Type castTypeToExpType(CastType castType) {
        return switch (castType) {
            case INT -> Exp.Type.INT;
            case FLOAT -> Exp.Type.FLOAT;
        };
    }

    public enum ReturnParam {
        VALUE,
        INDEX,
        RANK,
        COUNT,
        NONE,
        EXISTS,
        REVERSE_INDEX,
        REVERSE_RANK,
        KEY_VALUE,
        UNORDERED_MAP,
        ORDERED_MAP,
        KEY
    }

    public enum PathFunctionType {
        GET,
        COUNT,
        SIZE,
        CAST
    }

    public enum CastType {
        INT,
        FLOAT
    }
}
