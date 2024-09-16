package com.aerospike.dsl.model;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.cdt.CdtPart;
import com.aerospike.dsl.model.cdt.list.ListPart;
import com.aerospike.dsl.model.cdt.list.ListTypeDesignator;
import com.aerospike.dsl.model.cdt.map.MapPart;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.aerospike.dsl.model.PathFunction.PathFunctionType.SIZE;
import static com.aerospike.dsl.model.cdt.list.ListPart.ListPartType.LIST_TYPE_DESIGNATOR;
import static com.aerospike.dsl.model.cdt.map.MapPart.MapPartType.MAP_TYPE_DESIGNATOR;

@Getter
public class PathOperand extends AbstractPart {

    public PathOperand(Exp exp) {
        super(PartType.PATH_OPERAND, exp);
    }

    public static Exp processPath(BasePath basePath, PathFunction pathFunction) {
        Exp.Type valueType = null;
        PathFunction.ReturnParam returnParam = PathFunction.ReturnParam.VALUE;
        PathFunction.PathFunctionType pathFunctionType = PathFunction.PathFunctionType.GET;

        if (pathFunction != null) {
            if (pathFunction.getReturnParam() != null) returnParam = pathFunction.getReturnParam();
            if (pathFunction.getBinType() != null) {
                valueType = Exp.Type.valueOf(pathFunction.getBinType().toString());
            }
            if (pathFunction.getPathFunctionType() != null) pathFunctionType = pathFunction.getPathFunctionType();
        }

        List<AbstractPart> parts = basePath.getParts();
        if (!parts.isEmpty() || pathFunction != null) {
            basePath = checkForCdtTypeDesignator(basePath, pathFunctionType);
            AbstractPart lastPathPart = basePath.getParts().get(basePath.getParts().size() - 1);

            int cdtReturnType = ((CdtPart) lastPathPart).getReturnType(returnParam);

            return switch (pathFunctionType) {
                // CAST is the same as get with a different type
                case GET, COUNT, CAST -> processGet(basePath, lastPathPart, valueType, cdtReturnType);
                case SIZE -> processSize(basePath, lastPathPart, valueType, cdtReturnType);
            };
        }
        throw new AerospikeDSLException("Expecting other parts of path except bin");
    }

    private static boolean isCdtTypeDesignator(AbstractPart lastPathPart) {
        return (lastPathPart instanceof MapPart mapPart && mapPart.getMapPartType() == MAP_TYPE_DESIGNATOR)
                || (lastPathPart instanceof ListPart listPart && listPart.getListPartType() == LIST_TYPE_DESIGNATOR);
    }

    private static Exp processGet(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType, int cdtReturnType) {

        // Context can be empty
        CTX[] context = getContextArray(basePath.getParts(), false);

        valueType = findValueTypeIfNull(valueType, lastPathPart);
        return ((CdtPart) lastPathPart).constructExp(basePath, valueType, cdtReturnType, context);
    }

    private static CTX[] getContextArray(List<AbstractPart> parts, boolean includeLast) {
        // Nested (Context) map key access
        List<CTX> context = new ArrayList<>();

        for (int i = 0; i < parts.size(); i++) {
            if (!includeLast && i == parts.size() - 1) {
                // Skip last
                continue;
            }
            AbstractPart part = parts.get(i);
            context.add(((CdtPart) part).getContext());
        }
        return context.toArray(new CTX[0]);
    }

    private static Exp processSize(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType, int cdtReturnType) {
        BinPart bin = basePath.getBinPart();
        if (lastPathPart.getPartType() == PartType.LIST_PART) {
            ListPart listPart = (ListPart) lastPathPart;
            // list type designator "[]" can be either after bin name or after path
            if (listPart.getListPartType().equals(LIST_TYPE_DESIGNATOR)) {
                return getCdtExpSize(ListExp::size, basePath, lastPathPart, valueType, cdtReturnType);
            } else {
                // In size() the last element is considered context
                CTX[] context = getContextArray(basePath.getParts(), true);
                return ListExp.size(Exp.bin(bin.getBinName(), basePath.getBinType()), context);
            }
        } else if (lastPathPart.getPartType() == PartType.MAP_PART) {
            MapPart mapPart = (MapPart) lastPathPart;
            if (mapPart.getMapPartType().equals(MAP_TYPE_DESIGNATOR)) {
                return getCdtExpSize(MapExp::size, basePath, lastPathPart, valueType, cdtReturnType);
            } else {
                // In size() the last element is considered context
                CTX[] context = getContextArray(basePath.getParts(), true);
                return MapExp.size(Exp.bin(bin.getBinName(), basePath.getBinType()), context);
            }
        } else {
            return null;
        }
    }

    private static BasePath checkForCdtTypeDesignator(BasePath basePath, PathFunction.PathFunctionType pathFunctionType) {
        var parts = basePath.getParts();
        if (parts.isEmpty() || (mustHaveCdtDesignator(pathFunctionType, parts))) {
            // For cases like list.size() and map.size() -> no explicit type ([] is for List, {} is for Map)
            // No parts but with pathFunction (e.g. size()), in this case we apply default: List
            AbstractPart lastPathPart = new ListTypeDesignator();
            basePath.getParts().add(lastPathPart);
            return basePath;
        }
        return basePath;
    }

    private static boolean mustHaveCdtDesignator(PathFunction.PathFunctionType pathFunctionType,
                                                 List<AbstractPart> parts) {
        return pathFunctionType == SIZE && !isCdtTypeDesignator(parts.get(parts.size() - 1));
    }

    private static Exp getCdtExpSize(Function<Exp, Exp> function, BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType,
                                     int cdtReturnType) {
        // Context can be empty
        List<AbstractPart> partsUntilDesignator = !basePath.getParts().isEmpty()
                ? basePath.getParts().subList(0, basePath.getParts().size() - 1)
                : new ArrayList<>();
        CTX[] context = getContextArray(partsUntilDesignator, false);

        valueType = findValueTypeIfNull(valueType, lastPathPart);
        Exp cdtExp = ((CdtPart) lastPathPart).constructExp(basePath, valueType, cdtReturnType, context);
        return function.apply(cdtExp);
    }

    private static Exp.Type findValueTypeIfNull(Exp.Type valueType, AbstractPart lastPathPart) {
        /*
            Determine valueType according to this priority:
            1. From pathFunction (explicit type, casting) is preferred
            2. Type detection (lastPathPart.getExpType())
            3. Default INT
         */
        if (valueType == null) {
            if (lastPathPart.getExpType() != null) {
                return lastPathPart.getExpType();
            } else {
                return Exp.Type.INT;
            }
        }
        return valueType;
    }
}
