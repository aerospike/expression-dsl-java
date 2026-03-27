package com.aerospike.dsl.parts.path;

import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.cdt.CdtPart;
import lombok.Getter;

import java.util.List;

import static com.aerospike.dsl.parts.cdt.CdtPart.isCdtPart;
import static com.aerospike.dsl.util.PathOperandUtils.*;

@Getter
public class Path extends AbstractPart {

    private final BasePath basePath;
    private final PathFunction pathFunction;

    public Path(BasePath basePath, PathFunction pathFunction) {
        super(PartType.PATH_OPERAND);
        this.basePath = basePath;
        this.pathFunction = pathFunction;
    }

    public Exp processPath(BasePath basePath, PathFunction pathFunction) {
        List<AbstractPart> parts = basePath.getCdtParts();
        updateWithCdtTypeDesignator(basePath, pathFunction);
        AbstractPart lastPathPart = !parts.isEmpty() ? parts.get(parts.size() - 1) : null;
        pathFunction = processPathFunction(basePath, lastPathPart, pathFunction);
        Exp.Type valueType = processValueType(lastPathPart, pathFunction);

        int cdtReturnType = 0;
        if (lastPathPart != null && isCdtPart(lastPathPart)) {
            cdtReturnType = ((CdtPart) lastPathPart).getReturnType(pathFunction.getReturnParam());
        }

        if (lastPathPart != null) {
            Exp exp = switch (pathFunction.getPathFunctionType()) {
                case GET, COUNT, CAST -> processGet(basePath, lastPathPart, valueType, cdtReturnType);
                case SIZE -> processSize(basePath, lastPathPart, valueType, cdtReturnType);
            };
            if (pathFunction.getPathFunctionType() == PathFunction.PathFunctionType.CAST && exp != null) {
                exp = pathFunction.getBinType() == Exp.Type.FLOAT ? Exp.toFloat(exp) : Exp.toInt(exp);
            }
            return exp;
        }
        return null;
    }

    @Override
    public Exp getExp() {
        return processPath(basePath, pathFunction);
    }

    @Override
    public CTX[] getCtx() {
        return getContextArray(basePath.getCdtParts(), true);
    }
}
