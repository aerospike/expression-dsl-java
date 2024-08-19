package com.aerospike.dsl.model.cdt;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.model.BasePath;

public abstract class CdtPart extends AbstractPart {

    public CdtPart(PartType partType) {
        super(partType);
    }

    public abstract Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context);

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
}
