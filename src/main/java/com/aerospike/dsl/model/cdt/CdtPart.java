package com.aerospike.dsl.model.cdt;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.model.BasePath;
import com.aerospike.dsl.model.PathFunction;

public abstract class CdtPart extends AbstractPart {

    protected CdtPart(PartType partType) {
        super(partType);
    }

    public static Exp getExpVal(Exp.Type valueType, Object cdtValue) {
        return switch (valueType) {
            case BOOL -> Exp.val((Boolean) cdtValue);
            case INT -> Exp.val((Integer) cdtValue);
            case STRING -> Exp.val((String) cdtValue);
            case FLOAT -> Exp.val((Float) cdtValue);
            default -> throw new IllegalStateException(
                    "Get by value from a CDT: unexpected value '%s'".formatted(valueType));
        };
    }

    public abstract Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context);

    public CTX getContext() {
        // should print the subclass of the cdt type
        throw new AerospikeDSLException("Context is not supported for %s".formatted(this.getClass().getName()));
    }

    public abstract int getReturnType(PathFunction.ReturnParam returnParam);
}
