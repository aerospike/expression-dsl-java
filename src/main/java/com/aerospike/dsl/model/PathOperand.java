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
import com.aerospike.dsl.model.cdt.map.MapTypeDesignator;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.aerospike.dsl.model.PathFunction.PathFunctionType.COUNT;
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
            basePath = updateWithCdtTypeDesignator(basePath, pathFunction);
            AbstractPart lastPathPart = !parts.isEmpty() ? parts.get(parts.size() - 1) : null;

            int cdtReturnType = 0;
            if (lastPathPart instanceof CdtPart) cdtReturnType = ((CdtPart) lastPathPart).getReturnType(returnParam);

            return switch (pathFunctionType) {
                // CAST is the same as get with a different type
                case GET, COUNT, CAST -> processGet(basePath, lastPathPart, valueType, cdtReturnType, pathFunction);
                case SIZE -> processSize(basePath, lastPathPart, valueType, cdtReturnType, pathFunction);
            };
        }
        throw new AerospikeDSLException("Expecting other parts of path except bin");
    }

    private static boolean isPrevCdtPartAmbiguous(AbstractPart lastPart) {
        if (lastPart instanceof MapPart mapPart) { // check that lastPart is CDT Map
            // check relevant types
            return List.of(MapPart.MapPartType.INDEX, MapPart.MapPartType.RANK, MapPart.MapPartType.KEY)
                    .contains(mapPart.getMapPartType());
        }
        if (lastPart instanceof ListPart listPart) { // check that lastPart is CDT List
            // check relevant types
            return List.of(ListPart.ListPartType.INDEX, ListPart.ListPartType.RANK)
                    .contains(listPart.getListPartType());
        }
        return false;
    }

    private static Exp processGet(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType, int cdtReturnType,
                                  PathFunction pathFunction) {
        if (lastPathPart != null) {
            if (lastPathPart.getPartType() == PartType.LIST_PART) {
                return doProcessCdtGet(basePath, lastPathPart, valueType, cdtReturnType, pathFunction, (ListPart) lastPathPart);
            } else if (lastPathPart.getPartType() == PartType.MAP_PART) {
                return doProcessCdtGet(basePath, lastPathPart, valueType, cdtReturnType, pathFunction, (MapPart) lastPathPart);
            }
            return null;
        } else {
            // Context can be empty
            CTX[] context = getContextArray(basePath.getParts(), false);
            valueType = findValueTypeIfNull(valueType, lastPathPart, pathFunction);
            BinPart binPart = basePath.getBinPart();
            return Exp.bin(binPart.getBinName(), basePath.getBinType());
        }
    }

    private static Exp doProcessCdtGet(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType, int cdtReturnType,
                                       PathFunction pathFunction, CdtPart cdtPart) {
        // list type designator "[]" can be either after bin name or after path
        if (isListTypeDesignator(cdtPart) || isMapTypeDesignator(cdtPart)) {
            return constructCdtExp(basePath, lastPathPart, valueType, cdtReturnType, pathFunction);
        } else {
            // Context can be empty
            CTX[] context = getContextArray(basePath.getParts(), false);
            valueType = findValueTypeIfNull(valueType, lastPathPart, pathFunction);
            return ((CdtPart) lastPathPart).constructExp(basePath, valueType, cdtReturnType, context);
        }
    }

    private static boolean isListTypeDesignator(CdtPart cdtPart) {
        return cdtPart instanceof ListPart listPart && listPart.getListPartType().equals(LIST_TYPE_DESIGNATOR);
    }

    private static boolean isMapTypeDesignator(CdtPart cdtPart) {
        return cdtPart instanceof MapPart mapPart && mapPart.getMapPartType().equals(MAP_TYPE_DESIGNATOR);
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

    private static Exp processSize(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType, int cdtReturnType,
                                   PathFunction pathFunction) {
        BinPart bin = basePath.getBinPart();
        if (lastPathPart.getPartType() == PartType.LIST_PART) {
            ListPart listPart = (ListPart) lastPathPart;
            // list type designator "[]" can be either after bin name or after path
            if (listPart.getListPartType().equals(LIST_TYPE_DESIGNATOR)) {
                return getCdtExpFunction(ListExp::size, basePath, lastPathPart, valueType, cdtReturnType, pathFunction);
            } else {
                // In size() the last element is considered context
                CTX[] context = getContextArray(basePath.getParts(), true);
                return ListExp.size(Exp.bin(bin.getBinName(), basePath.getBinType()), context);
            }
        } else if (lastPathPart.getPartType() == PartType.MAP_PART) {
            MapPart mapPart = (MapPart) lastPathPart;
            if (mapPart.getMapPartType().equals(MAP_TYPE_DESIGNATOR)) {
                return getCdtExpFunction(MapExp::size, basePath, lastPathPart, valueType, cdtReturnType, pathFunction);
            } else {
                // In size() the last element is considered context
                CTX[] context = getContextArray(basePath.getParts(), true);
                return MapExp.size(Exp.bin(bin.getBinName(), basePath.getBinType()), context);
            }
        } else {
            return null;
        }
    }

    private static BasePath updateWithCdtTypeDesignator(BasePath basePath, PathFunction pathFunction) {
        if (mustHaveCdtDesignator(pathFunction, basePath.getParts())) {
            // For cases like list.size() and map.size() when no explicit type ([] is for List, {} is for Map)
            // If no parts but with pathFunction (e.g. $.bin.size()) - in this case we apply default List
            AbstractPart lastPathPart = new ListTypeDesignator();
            basePath.getParts().add(lastPathPart);
            return basePath;
        }
        return basePath;
    }

    private static boolean mustHaveCdtDesignator(PathFunction pathFunction,
                                                 List<AbstractPart> parts) {
        // if existing path function type is SIZE or COUNT,
        // or if path function type is GET with return type COUNT
        // and parts are empty (only bin) or previous CDT part is ambiguous (index, rank or map key)
        return pathFunction != null
                && (List.of(SIZE, COUNT).contains(pathFunction.getPathFunctionType())
                || pathFunctionIsGetWithCount(pathFunction))
                && (parts.isEmpty() || (isPrevCdtPartAmbiguous(parts.get(parts.size() - 1))));
    }

    private static boolean pathFunctionIsGetWithCount(PathFunction pathFunction) {
        return pathFunction.getPathFunctionType() == PathFunction.PathFunctionType.GET
                && pathFunction.getReturnParam() == PathFunction.ReturnParam.COUNT;
    }

    private static Exp constructCdtExp(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType,
                                       int cdtReturnType, PathFunction pathFunction) {
        // Context can be empty
        List<AbstractPart> partsUpToDesignator = !basePath.getParts().isEmpty()
                ? basePath.getParts().subList(0, basePath.getParts().size() - 1)
                : new ArrayList<>();
        CTX[] context = getContextArray(partsUpToDesignator, false);

        valueType = findValueTypeIfNull(valueType, lastPathPart, pathFunction);
        return ((CdtPart) lastPathPart).constructExp(basePath, valueType, cdtReturnType, context);
    }

    private static Exp getCdtExpFunction(Function<Exp, Exp> function, BasePath basePath, AbstractPart lastPathPart,
                                         Exp.Type valueType, int cdtReturnType, PathFunction pathFunction) {
        Exp cdtExp = constructCdtExp(basePath, lastPathPart, valueType, cdtReturnType, pathFunction);
        return function.apply(cdtExp);
    }

    private static Exp.Type findValueTypeIfNull(Exp.Type valueType, AbstractPart lastPathPart,
                                                PathFunction pathFunction) {
        /*
            Determine valueType according to this priority:
            1. From pathFunction (explicit type, casting) is preferred
            2. Type detection (lastPathPart.getExpType())
            3. Default INT
         */
        if (valueType == null) {
            if (lastPathPart.getExpType() != null) {
                return lastPathPart.getExpType();
            } else if (pathFunction.getPathFunctionType() == COUNT) {
                return getValueTypeForCount(lastPathPart);
            } else {
                return Exp.Type.INT;
            }
        }
        return valueType;
    }

    private static Exp.Type getValueTypeForCount(AbstractPart lastPathPart) {
        if (lastPathPart instanceof ListTypeDesignator) {
                return Exp.Type.LIST;
            } else if (lastPathPart instanceof MapTypeDesignator) {
                return Exp.Type.MAP;
            }
        return Exp.Type.INT;
    }
}
