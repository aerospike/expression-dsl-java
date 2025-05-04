package com.aerospike.dsl.parts.cdt;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.exceptions.ParseException;
import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.path.BasePath;
import com.aerospike.dsl.parts.path.PathFunction;

public abstract class CdtPart extends AbstractPart {

    protected CdtPart(PartType partType) {
        super(partType);
    }

    public abstract Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context);

    public CTX getContext() {
        // should print the subclass of the cdt type
        throw new ParseException("Context is not supported for %s".formatted(this.getClass().getName()));
    }

    public abstract int getReturnType(PathFunction.ReturnParam returnParam);
}
