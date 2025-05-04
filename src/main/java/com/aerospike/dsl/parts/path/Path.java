package com.aerospike.dsl.parts.path;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.cdt.CdtPart;
import lombok.Getter;

import java.util.List;

import static com.aerospike.dsl.util.PathOperandUtils.processGet;
import static com.aerospike.dsl.util.PathOperandUtils.processPathFunction;
import static com.aerospike.dsl.util.PathOperandUtils.processSize;
import static com.aerospike.dsl.util.PathOperandUtils.processValueType;
import static com.aerospike.dsl.util.PathOperandUtils.updateWithCdtTypeDesignator;

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
        List<AbstractPart> parts = basePath.getParts();
        updateWithCdtTypeDesignator(basePath, pathFunction);
        AbstractPart lastPathPart = !parts.isEmpty() ? parts.get(parts.size() - 1) : null;
        pathFunction = processPathFunction(basePath, lastPathPart, pathFunction);
        Exp.Type valueType = processValueType(lastPathPart, pathFunction);

        int cdtReturnType = 0;
        if (lastPathPart instanceof CdtPart lastPart) cdtReturnType =
                lastPart.getReturnType(pathFunction.getReturnParam());

        if (lastPathPart != null) { // only if there are other parts except a bin
            return switch (pathFunction.getPathFunctionType()) {
                // CAST is the same as GET with a different type
                case GET, COUNT, CAST -> processGet(basePath, lastPathPart, valueType, cdtReturnType);
                case SIZE -> processSize(basePath, lastPathPart, valueType, cdtReturnType);
            };
        }
        return null;
    }

    public Exp getExp() {
        return processPath(basePath, pathFunction);
    }
}
