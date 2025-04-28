package com.aerospike.dsl.util;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.part.AbstractPart;
import com.aerospike.dsl.part.path.BasePath;
import com.aerospike.dsl.part.path.BinPart;
import com.aerospike.dsl.part.path.PathFunction;
import com.aerospike.dsl.part.cdt.CdtPart;
import com.aerospike.dsl.part.cdt.list.ListPart;
import com.aerospike.dsl.part.cdt.list.ListTypeDesignator;
import com.aerospike.dsl.part.cdt.map.MapPart;
import com.aerospike.dsl.part.cdt.map.MapTypeDesignator;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static com.aerospike.dsl.part.AbstractPart.PartType.LIST_PART;
import static com.aerospike.dsl.part.AbstractPart.PartType.MAP_PART;
import static com.aerospike.dsl.part.path.PathFunction.PathFunctionType.*;
import static com.aerospike.dsl.part.cdt.list.ListPart.ListPartType.*;
import static com.aerospike.dsl.part.cdt.map.MapPart.MapPartType.MAP_TYPE_DESIGNATOR;

@UtilityClass
public class PathOperandUtils {

    public static Exp.Type processValueType(AbstractPart lastPathPart, PathFunction pathFunction) {
        // there is always a path function with non-null function type and return param
        if (pathFunction.getBinType() == null) {
            if (isListTypeDesignator(lastPathPart)) {
                return Exp.Type.LIST;
            } else if (isMapTypeDesignator(lastPathPart)) {
                return Exp.Type.MAP;
            } else {
                return findValueType(lastPathPart, pathFunction.getPathFunctionType());
            }
        }
        return Exp.Type.valueOf(pathFunction.getBinType().toString());
    }

    public static PathFunction processPathFunction(BasePath basePath, AbstractPart lastPathPart,
                                                   PathFunction pathFunction) {
        if (pathFunction == null) {
            // a default path function
            return new PathFunction(GET, PathFunction.ReturnParam.VALUE, null);
        }

        // Use size() operation for non-range CDT count
        PathFunction.ReturnParam defaultReturnParam = PathFunction.ReturnParam.VALUE;
        if (pathFunction.getPathFunctionType() == COUNT) {
            // If there is only a bin or only a CDT designator
            if (basePath.getParts().isEmpty() || containOnlyCdtDesignator(basePath.getParts())) {
                PathFunction.PathFunctionType type = COUNT;
                if (basePath.getBinType() == Exp.Type.LIST || basePath.getBinType() == Exp.Type.MAP) {
                    type = SIZE;
                }
                return new PathFunction(type, defaultReturnParam, null);
            }

            // If the last path part is a CDT type designator, get the previous part
            if (isListTypeDesignator(lastPathPart) || isMapTypeDesignator(lastPathPart)) {
                AbstractPart partBeforeDesignator =
                        getPartOrNull(basePath.getParts(), basePath.getParts().size() - 2);
                if (partBeforeDesignator != null) lastPathPart = partBeforeDesignator;
            }

            // If the last path part is a List index or rank
            if (lastPathPart.getPartType() == LIST_PART) {
                ListPart listPart = (ListPart) lastPathPart;
                if (listPart.getListPartType() == INDEX || listPart.getListPartType() == RANK) {
                    return new PathFunction(SIZE, defaultReturnParam, null);
                }
            }

            // If the last path part is a Map index, rank or key
            if (lastPathPart.getPartType() == MAP_PART) {
                MapPart mapPart = (MapPart) lastPathPart;
                if (mapPart.getMapPartType() == MapPart.MapPartType.INDEX
                        || mapPart.getMapPartType() == MapPart.MapPartType.RANK
                        || mapPart.getMapPartType() == MapPart.MapPartType.KEY) {
                    return new PathFunction(SIZE, defaultReturnParam, null);
                }
            }
        }

        // Apply defaults
        if (pathFunction.getReturnParam() == null) pathFunction =
                new PathFunction(pathFunction.getPathFunctionType(), defaultReturnParam, pathFunction.getBinType());
        if (pathFunction.getPathFunctionType() == null) pathFunction =
                new PathFunction(GET, pathFunction.getReturnParam(), pathFunction.getBinType());

        return pathFunction;
    }

    private static AbstractPart getPartOrNull(List<AbstractPart> parts, int idx) {
        return idx >= 0 ? parts.get(idx) : null;
    }

    private static boolean containOnlyCdtDesignator(List<AbstractPart> parts) {
        return parts.size() == 1 && (isListTypeDesignator(parts.get(0)) || isMapTypeDesignator(parts.get(0)));
    }

    private static boolean isPrevCdtPartAmbiguous(AbstractPart lastPart) {
        if (lastPart instanceof MapPart mapPart) { // check that lastPart is CDT Map
            // check relevant types
            return List.of(MapPart.MapPartType.INDEX, MapPart.MapPartType.RANK, MapPart.MapPartType.KEY,
                            MapPart.MapPartType.VALUE)
                    .contains(mapPart.getMapPartType());
        }
        if (lastPart instanceof ListPart listPart) { // check that lastPart is CDT List
            // check relevant types
            return List.of(INDEX, RANK, ListPart.ListPartType.VALUE)
                    .contains(listPart.getListPartType());
        }
        return false;
    }

    public static Exp processGet(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType, int cdtReturnType) {
        if (lastPathPart.getPartType() == LIST_PART) {
            return doProcessCdtGet(basePath, lastPathPart, valueType, cdtReturnType, (ListPart) lastPathPart);
        } else if (lastPathPart.getPartType() == MAP_PART) {
            return doProcessCdtGet(basePath, lastPathPart, valueType, cdtReturnType, (MapPart) lastPathPart);
        }
        throw new UnsupportedOperationException(
                String.format("Path part type %s is not supported", lastPathPart.getPartType()));
    }

    private static Exp doProcessCdtGet(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType,
                                       int cdtReturnType, CdtPart cdtPart) {
        // list type designator "[]" can be either after bin name or after path
        if (isListTypeDesignator(cdtPart) || isMapTypeDesignator(cdtPart)) {
            return constructCdtExp(basePath, lastPathPart, valueType, cdtReturnType);
        }

        // Context can be empty
        CTX[] context = getContextArray(basePath.getParts(), false);
        return ((CdtPart) lastPathPart).constructExp(basePath, valueType, cdtReturnType, context);
    }

    private static boolean isListTypeDesignator(AbstractPart cdtPart) {
        return cdtPart instanceof ListPart listPart && listPart.getListPartType().equals(LIST_TYPE_DESIGNATOR);
    }

    private static boolean isMapTypeDesignator(AbstractPart cdtPart) {
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

    public static Exp processSize(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType, int cdtReturnType) {
        if (lastPathPart.getPartType() == LIST_PART) {
            return processListPartSize(basePath, lastPathPart, valueType, cdtReturnType);
        } else if (lastPathPart.getPartType() == MAP_PART) {
            return processMapPartSize(basePath, lastPathPart, valueType, cdtReturnType);
        }
        throw new UnsupportedOperationException(
                String.format("Path part type %s is not supported", lastPathPart.getPartType()));
    }

    private static Exp processListPartSize(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType,
                                           int cdtReturnType) {
        BinPart bin = basePath.getBinPart();
        ListPart listPart = (ListPart) lastPathPart;

        // list type designator "[]" can be either after bin name or after path
        if (listPart.getListPartType().equals(LIST_TYPE_DESIGNATOR)) {
            return getCdtExpFunction(ListExp::size, basePath, lastPathPart, valueType, cdtReturnType);
        }
        // In size() the last element is considered context
        CTX[] context = getContextArray(basePath.getParts(), true);
        return ListExp.size(Exp.bin(bin.getBinName(), basePath.getBinType()), context);
    }

    private static Exp processMapPartSize(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType,
                                          int cdtReturnType) {
        BinPart bin = basePath.getBinPart();
        MapPart mapPart = (MapPart) lastPathPart;

        if (mapPart.getMapPartType().equals(MAP_TYPE_DESIGNATOR)) {
            return getCdtExpFunction(MapExp::size, basePath, lastPathPart, valueType, cdtReturnType);
        }
        // In size() the last element is considered context
        CTX[] context = getContextArray(basePath.getParts(), true);
        return MapExp.size(Exp.bin(bin.getBinName(), basePath.getBinType()), context);
    }

    public static void updateWithCdtTypeDesignator(BasePath basePath, PathFunction pathFunction) {
        if (mustHaveCdtDesignator(pathFunction, basePath.getParts())) {
            // For cases like list.count() and map.count() with no explicit designator ([] is for List, {} is for Map)
            // When the last path part is CDT and potentially ambiguous, we apply List designator by default
            AbstractPart lastPathPart = new ListTypeDesignator();
            basePath.getParts().add(lastPathPart);
        }
    }

    private static boolean mustHaveCdtDesignator(PathFunction pathFunction,
                                                 List<AbstractPart> parts) {
        // if existing path function type is SIZE or COUNT
        // and parts are empty (only bin) or previous CDT part is ambiguous (CDT index, rank or map key)
        if (pathFunction == null) return false;
        PathFunction.PathFunctionType type = pathFunction.getPathFunctionType();
        return (List.of(SIZE, COUNT).contains(type) || pathFunctionIsGetWithCount(pathFunction))
                && (parts.isEmpty() || (isPrevCdtPartAmbiguous(parts.get(parts.size() - 1))));
    }

    private static boolean pathFunctionIsGetWithCount(PathFunction pathFunction) {
        return pathFunction.getPathFunctionType() == GET
                && pathFunction.getReturnParam() == PathFunction.ReturnParam.COUNT;
    }

    private static Exp constructCdtExp(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType,
                                       int cdtReturnType) {
        // Context can be empty
        List<AbstractPart> partsUpToDesignator = basePath.getParts().isEmpty()
                ? new ArrayList<>()
                : basePath.getParts().subList(0, basePath.getParts().size() - 1);
        CTX[] context = getContextArray(partsUpToDesignator, false);

        return ((CdtPart) lastPathPart).constructExp(basePath, valueType, cdtReturnType, context);
    }

    private static Exp getCdtExpFunction(UnaryOperator<Exp> operator, BasePath basePath, AbstractPart lastPathPart,
                                         Exp.Type valueType, int cdtReturnType) {
        Exp cdtExp = constructCdtExp(basePath, lastPathPart, valueType, cdtReturnType);
        return operator.apply(cdtExp);
    }

    private static Exp.Type findValueType(AbstractPart lastPathPart, PathFunction.PathFunctionType pathFunctionType) {
        /*
            Determine valueType based on
            1. The last path part
            2. Path function type
            3. Default type
         */
        if (lastPathPart != null && lastPathPart.getExpType() != null) {
            return lastPathPart.getExpType();
        } else if (pathFunctionType == COUNT) {
            return getValueTypeForCount(lastPathPart);
        }
        return TypeUtils.getDefaultType(lastPathPart);
    }

    private static Exp.Type getValueTypeForCount(AbstractPart lastPathPart) {
        if (lastPathPart instanceof ListTypeDesignator) {
            return Exp.Type.LIST;
        } else if (lastPathPart instanceof MapTypeDesignator) {
            return Exp.Type.MAP;
        }
        return TypeUtils.getDefaultTypeForCount();
    }
}
