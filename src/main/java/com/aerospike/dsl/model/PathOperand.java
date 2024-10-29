package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.cdt.CdtPart;
import lombok.Getter;

import java.util.List;

import static com.aerospike.dsl.util.PathOperandUtils.*;

@Getter
public class PathOperand extends AbstractPart {

    public PathOperand(Exp exp) {
        super(PartType.PATH_OPERAND, exp);
    }

    public static Exp processPath(BasePath basePath, PathFunction pathFunction) {
        List<AbstractPart> parts = basePath.getParts();
        basePath = updateWithCdtTypeDesignator(basePath, pathFunction);
        AbstractPart lastPathPart = !parts.isEmpty() ? parts.get(parts.size() - 1) : null;
        pathFunction = processPathFunction(basePath, lastPathPart, pathFunction);
        Exp.Type valueType = processValueType(lastPathPart, pathFunction);

        int cdtReturnType = 0;
        if (lastPathPart instanceof CdtPart) cdtReturnType =
                ((CdtPart) lastPathPart).getReturnType(pathFunction.getReturnParam());

        if (lastPathPart != null) { // only if there are other parts except a bin
            return switch (pathFunction.getPathFunctionType()) {
                // CAST is the same as GET with a different type
                case GET, COUNT, CAST -> processGet(basePath, lastPathPart, valueType, cdtReturnType);
                case SIZE -> processSize(basePath, lastPathPart, valueType, cdtReturnType);
            };
        }
        return null;
    }
}
