package com.aerospike.dsl.part.cdt;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.part.AbstractPart;
import com.aerospike.dsl.part.path.BasePath;
import com.aerospike.dsl.part.path.PathFunction;

public abstract class CdtPart extends AbstractPart {

    protected CdtPart(PartType partType) {
        super(partType);
    }

    public abstract Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context);

    public CTX getContext() {
        // should print the subclass of the cdt type
        throw new AerospikeDSLException("Context is not supported for %s".formatted(this.getClass().getName()));
    }

    public abstract int getReturnType(PathFunction.ReturnParam returnParam);
}
