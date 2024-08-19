package com.aerospike.dsl.model;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.list.ListPart;
import com.aerospike.dsl.model.map.MapPart;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

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

        int cdtReturnType = switch (returnParam) {
            case VALUE -> ListReturnType.VALUE; // same as MapReturnType.VALUE
            case COUNT, NONE -> ListReturnType.COUNT; // same as MapReturnType.COUNT
        };

        List<AbstractPart> parts = basePath.getParts();
        AbstractPart lastPathPart;
        if (!parts.isEmpty() || pathFunction != null) {
            if (!parts.isEmpty()) {
                lastPathPart = parts.get(parts.size() - 1);
            } else {
                // For cases like map.size() -> we don't know that it is a map (in lists we have [] for list bins)
                // No parts but with pathFunction (e.g. size()), in this case we will create synthetic Map part
                // Key doesn't matter in this case, we look at the base part
                lastPathPart = MapPart.builder().setMapKeyBin(basePath.getBinPart().getBinName()).build();
                basePath.getParts().add(lastPathPart);
            }

            return switch (pathFunctionType) {
                // CAST is the same as get with a different type
                case GET, COUNT, CAST -> processGet(basePath, lastPathPart, valueType, cdtReturnType);
                case SIZE -> processSize(basePath, lastPathPart);
            };
        }
        throw new AerospikeDSLException("Expecting other parts of path except bin");
    }

    private static Exp processGet(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType, int cdtReturnType) {
        // Context can be empty
        CTX[] context = getContextArray(basePath, false);
        BinPart bin = basePath.getBinPart();

        /*
            Determine valueType according to this priority:
            1. From pathFunction (explicit type, casting) is preferred
            2. Type detection (lastPathPart.getExpType())
            3. Default INT
         */
        if (valueType == null) {
            if (lastPathPart.getExpType() != null) {
                valueType = lastPathPart.getExpType();
            } else {
                valueType = Exp.Type.INT;
            }
        }

        if (lastPathPart.getPartType() == PartType.LIST_PART) {
            ListPart listLastPart = (ListPart) lastPathPart;

            return switch (listLastPart.getListPartType()) {
                case BIN -> Exp.listBin(bin.getBinName());
                case INDEX -> ListExp.getByIndex(cdtReturnType, valueType, Exp.val(listLastPart.getListIndex()),
                        Exp.bin(bin.getBinName(), getBinType(basePath)), context);
                case VALUE -> {
                    Exp value = getExpVal(valueType, listLastPart.getListValue());
                    yield ListExp.getByValue(cdtReturnType, value, Exp.bin(bin.getBinName(),
                            getBinType(basePath)), context);
                }
                case RANK -> ListExp.getByRank(cdtReturnType, valueType, Exp.val(listLastPart.getListRank()),
                        Exp.bin(bin.getBinName(), getBinType(basePath)), context);
                case INDEX_RANGE -> {
                    if (listLastPart.getListIndexRange().isInverted()) {
                        cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
                    }
                    Exp start = Exp.val(listLastPart.getListIndexRange().getStart());
                    Exp count = null;
                    if (listLastPart.getListIndexRange().getCount() != null) {
                        count = Exp.val(listLastPart.getListIndexRange().getCount());
                    }
                    if (count == null) {
                        yield ListExp.getByIndexRange(cdtReturnType, start, Exp.bin(bin.getBinName(),
                                getBinType(basePath)), context);
                    } else {
                        yield ListExp.getByIndexRange(cdtReturnType, start, count, Exp.bin(bin.getBinName(),
                                getBinType(basePath)), context);
                    }
                }
                case VALUE_LIST -> {
                    if (listLastPart.getListValueList().isInverted()) {
                        cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
                    }
                    yield ListExp.getByValueList(cdtReturnType, Exp.val(listLastPart.getListValueList().getValueList()),
                            Exp.bin(bin.getBinName(), getBinType(basePath)), context);
                }
                case VALUE_RANGE -> {
                    if (listLastPart.getListValueRange().isInverted()) {
                        cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
                    }

                    Exp start = Exp.val(listLastPart.getListValueRange().getStart());
                    Exp end = null;

                    if (listLastPart.getListValueRange().getEnd() != null) {
                        end = Exp.val(listLastPart.getListValueRange().getEnd());
                    }
                    yield ListExp.getByValueRange(cdtReturnType, start, end, Exp.bin(bin.getBinName(),
                            getBinType(basePath)), context);
                }
                case RANK_RANGE -> {
                    if (listLastPart.getListRankRange().isInverted()) {
                        cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
                    }
                    Exp start = Exp.val(listLastPart.getListRankRange().getStart());
                    Exp count = null;
                    if (listLastPart.getListRankRange().getCount() != null) {
                        count = Exp.val(listLastPart.getListRankRange().getCount());
                    }
                    if (count == null) {
                        yield ListExp.getByRankRange(cdtReturnType, start, Exp.bin(bin.getBinName(),
                                getBinType(basePath)), context);
                    } else {
                        yield ListExp.getByRankRange(cdtReturnType, start, count, Exp.bin(bin.getBinName(),
                                getBinType(basePath)), context);
                    }
                }
            };
        } else if (lastPathPart.getPartType() == PartType.MAP_PART) {
            MapPart mapLastPart = (MapPart) lastPathPart;

            return switch (mapLastPart.getMapPartType()) {
                case BIN -> Exp.mapBin(bin.getBinName());
                case KEY -> MapExp.getByKey(cdtReturnType, valueType,
                        Exp.val(mapLastPart.getMapKey()), Exp.bin(bin.getBinName(), getBinType(basePath)), context);
                case INDEX -> MapExp.getByIndex(cdtReturnType, valueType, Exp.val(mapLastPart.getMapIndex()),
                        Exp.bin(bin.getBinName(), getBinType(basePath)), context);
                case VALUE -> {
                    Exp value = getExpVal(valueType, mapLastPart.getMapValue());
                    yield MapExp.getByValue(cdtReturnType, value, Exp.bin(bin.getBinName(),
                            getBinType(basePath)), context);
                }
                case RANK -> MapExp.getByRank(cdtReturnType, valueType, Exp.val(mapLastPart.getMapRank()),
                        Exp.bin(bin.getBinName(), getBinType(basePath)), context);
                case KEY_RANGE -> {
                    if (mapLastPart.getMapKeyRange().isInverted()) {
                        cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
                    }
                    Exp start = Exp.val(mapLastPart.getMapKeyRange().getStart());
                    Exp end = null;
                    if (mapLastPart.getMapKeyRange().getEnd() != null) {
                        end = Exp.val(mapLastPart.getMapKeyRange().getEnd());
                    }
                    yield MapExp.getByKeyRange(cdtReturnType, start, end, Exp.bin(bin.getBinName(),
                            getBinType(basePath)), context);
                }
                case KEY_LIST -> {
                    if (mapLastPart.getMapKeyList().isInverted()) {
                        cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
                    }
                    yield MapExp.getByKeyList(cdtReturnType, Exp.val(mapLastPart.getMapKeyList().getKeyList()),
                            Exp.bin(bin.getBinName(), getBinType(basePath)), context);
                }
                case INDEX_RANGE -> {
                    if (mapLastPart.getMapIndexRange().isInverted()) {
                        cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
                    }
                    Exp start = Exp.val(mapLastPart.getMapIndexRange().getStart());
                    Exp count = null;
                    if (mapLastPart.getMapIndexRange().getCount() != null) {
                        count = Exp.val(mapLastPart.getMapIndexRange().getCount());
                    }
                    if (count == null) {
                        yield MapExp.getByIndexRange(cdtReturnType, start, Exp.bin(bin.getBinName(),
                                getBinType(basePath)), context);
                    } else {
                        yield MapExp.getByIndexRange(cdtReturnType, start, count, Exp.bin(bin.getBinName(),
                                getBinType(basePath)), context);
                    }
                }
                case VALUE_LIST -> {
                    if (mapLastPart.getMapValueList().isInverted()) {
                        cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
                    }
                    yield MapExp.getByValueList(cdtReturnType, Exp.val(mapLastPart.getMapValueList().getValueList()),
                            Exp.bin(bin.getBinName(), getBinType(basePath)), context);
                }
                case VALUE_RANGE -> {
                    if (mapLastPart.getMapValueRange().isInverted()) {
                        cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
                    }

                    Exp start = Exp.val(mapLastPart.getMapValueRange().getStart());
                    Exp end = null;

                    if (mapLastPart.getMapValueRange().getEnd() != null) {
                        end = Exp.val(mapLastPart.getMapValueRange().getEnd());
                    }
                    yield MapExp.getByValueRange(cdtReturnType, start, end, Exp.bin(bin.getBinName(),
                            getBinType(basePath)), context);
                }
                case RANK_RANGE -> {
                    if (mapLastPart.getMapRankRange().isInverted()) {
                        cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
                    }
                    Exp start = Exp.val(mapLastPart.getMapRankRange().getStart());
                    Exp count = null;
                    if (mapLastPart.getMapRankRange().getCount() != null) {
                        count = Exp.val(mapLastPart.getMapRankRange().getCount());
                    }
                    if (count == null) {
                        yield MapExp.getByRankRange(cdtReturnType, start, Exp.bin(bin.getBinName(),
                                getBinType(basePath)), context);
                    } else {
                        yield MapExp.getByRankRange(cdtReturnType, start, count, Exp.bin(bin.getBinName(),
                                getBinType(basePath)), context);
                    }
                }
            };
        } else {
            return null; // TODO
        }
    }

    private static CTX[] getContextArray(BasePath basePath, boolean includeLast) {
        // Nested (Context) map key access
        List<CTX> context = new ArrayList<>();

        for (int i = 0; i < basePath.getParts().size(); i++) {
            if (!includeLast && i == basePath.getParts().size() - 1) {
                // Skip last
                continue;
            }
            AbstractPart part = basePath.getParts().get(i);
            switch (part.getPartType()) {
                case LIST_PART -> {
                    ListPart listPart = (ListPart) part;
                    switch (listPart.getListPartType()) {
                        case INDEX -> context.add(CTX.listIndex(listPart.getListIndex()));
                        case VALUE -> context.add(CTX.listValue(Value.get(listPart.getListValue())));
                        case RANK -> context.add(CTX.listRank(listPart.getListRank()));
                        default -> throw new AerospikeDSLException("Unsupported List Part in Context: %s."
                                .formatted(listPart.getListPartType()));
                    }
                }
                case MAP_PART -> {
                    MapPart mapPart = (MapPart) part;
                    switch (mapPart.getMapPartType()) {
                        case KEY -> context.add(CTX.mapKey(Value.get(mapPart.getMapKey())));
                        case INDEX -> context.add(CTX.mapIndex(mapPart.getMapIndex()));
                        case VALUE -> context.add(CTX.mapValue(Value.get(mapPart.getMapValue())));
                        case RANK -> context.add(CTX.mapRank(mapPart.getMapRank()));
                    }
                }
            }
        }
        return context.toArray(new CTX[0]);
    }

    private static Exp getExpVal(Exp.Type valueType, Object cdtValue) {
        return switch (valueType) {
            case BOOL -> Exp.val((Boolean) cdtValue);
            case INT -> Exp.val((Integer) cdtValue);
            case STRING -> Exp.val((String) cdtValue);
            case FLOAT -> Exp.val((Float) cdtValue);
            default -> throw new IllegalStateException(
                    "Get by value from a CDT: unexpected value '%s'".formatted(valueType));
        };
    }

    private static Exp processSize(BasePath basePath, AbstractPart lastPathPart) {
        BinPart bin = basePath.getBinPart();
        if (lastPathPart.getPartType() == PartType.LIST_PART) {
            ListPart list = (ListPart) lastPathPart;
            if (list.getListPartType().equals(ListPart.ListPartType.BIN)) {
                return ListExp.size(Exp.bin(bin.getBinName(), getBinType(basePath)));
            } else {
                // In size() the last element is considered context
                CTX[] context = getContextArray(basePath, true);
                return ListExp.size(Exp.bin(bin.getBinName(), getBinType(basePath)), context);
            }
        } else if (lastPathPart.getPartType() == PartType.MAP_PART) {
            MapPart map = (MapPart) lastPathPart;
            if (map.getMapPartType().equals(MapPart.MapPartType.BIN)) {
                return MapExp.size(Exp.bin(bin.getBinName(), getBinType(basePath)));
            } else {
                // In size() the last element is considered context
                CTX[] context = getContextArray(basePath, true);
                return MapExp.size(Exp.bin(bin.getBinName(), getBinType(basePath)), context);
            }
        } else {
            return null;
        }
    }

    // Bin type is determined by the base path's first element
    private static Exp.Type getBinType(BasePath basePath) {
        return switch (basePath.getParts().get(0).getPartType()) {
            case MAP_PART -> Exp.Type.MAP;
            case LIST_PART -> Exp.Type.LIST;
            default -> null;
        };
    }
}
