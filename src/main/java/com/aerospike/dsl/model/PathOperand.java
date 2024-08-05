package com.aerospike.dsl.model;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class PathOperand extends AbstractPart {

    public PathOperand(Exp exp) {
        super(PartType.PATH_OPERAND, exp);
    }

    public static Exp processPath(BasePath basePath, PathFunction pathFunction) {
        Exp.Type binType = Exp.Type.INT;
        PathFunction.ReturnParam returnParam = PathFunction.ReturnParam.VALUE;
        PathFunction.PathFunctionType pathFunctionType = PathFunction.PathFunctionType.GET;

        if (pathFunction != null) {
            if (pathFunction.getReturnParam() != null) returnParam = pathFunction.getReturnParam();
            if (pathFunction.getBinType() != null) binType = pathFunction.getBinType();
            if (pathFunction.getPathFunctionType() != null) pathFunctionType = pathFunction.getPathFunctionType();
        }

        Exp.Type valueType = Exp.Type.valueOf(binType.toString());

        int listReturnType = switch (returnParam) {
            case VALUE -> ListReturnType.VALUE;
            case COUNT, NONE -> ListReturnType.COUNT;
        };

        List<AbstractPart> parts = basePath.getParts();
        AbstractPart lastPathPart;
        if (!parts.isEmpty() || pathFunction != null) {
            if (!parts.isEmpty()) {
                lastPathPart = parts.get(parts.size() - 1);
            } else {
                // No parts but with pathFunction (e.g. size()), in this case we will create synthetic Map part
                // Key doesn't matter in this case, we look at the base part
                lastPathPart = new MapPart(null);
                basePath.getParts().add(lastPathPart);
            }

            return switch (pathFunctionType) {
                // CAST is the same as get with a different type
                case GET, COUNT, CAST -> processGet(basePath, lastPathPart, valueType, listReturnType);
                case SIZE -> processSize(basePath, lastPathPart, valueType);
            };
        }
        throw new AerospikeDSLException("Expecting other parts of path except bin");
    }

    private static Exp processGet(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType, int listReturnType) {
        if (lastPathPart.getPartType() == PartType.LIST_PART) {
            ListPart list = (ListPart) lastPathPart;
            BinPart bin = basePath.getBinPart();
            CTX[] context = getContextArray(basePath, false);

            return switch (list.getListPathType()) {
                case BIN -> Exp.listBin(bin.getBinName());
                case INDEX -> {
                    if (context.length == 0) {
                        yield ListExp.getByIndex(listReturnType, valueType, Exp.val(list.getListIndex()),
                                Exp.bin(bin.getBinName(), getBinType(basePath)));
                    } else {
                        yield ListExp.getByIndex(listReturnType, valueType, Exp.val(list.getListIndex()),
                                Exp.bin(bin.getBinName(), getBinType(basePath)), context);
                    }
                }
                case VALUE -> {
                    Exp value = getExpVal(valueType, list.getListValue());
                    if (context.length == 0) {
                        yield ListExp.getByValue(listReturnType, value, Exp.bin(bin.getBinName(), getBinType(basePath)));
                    } else {
                        yield ListExp.getByValue(listReturnType, value, Exp.bin(bin.getBinName(),
                                getBinType(basePath)), context);
                    }
                }
                case RANK -> {
                    if (context.length == 0) {
                        yield ListExp.getByRank(listReturnType, valueType, Exp.val(list.getListRank()),
                                Exp.bin(bin.getBinName(), getBinType(basePath)));
                    } else {
                        yield ListExp.getByRank(listReturnType, valueType, Exp.val(list.getListRank()),
                                Exp.bin(bin.getBinName(), getBinType(basePath)), context);
                    }
                }
            };
        } else if (lastPathPart.getPartType() == PartType.MAP_PART) {
            MapPart mapLastPart = (MapPart) lastPathPart;
            BinPart bin = basePath.getBinPart();

            if (basePath.getParts().size() == 1) {
                // Single map key access
                return MapExp.getByKey(listReturnType, valueType,
                        Exp.val(mapLastPart.getKey()), Exp.bin(bin.getBinName(), getBinType(basePath)));
            } else {
                // Context map access
                CTX[] context = getContextArray(basePath, false);
                return MapExp.getByKey(listReturnType, valueType,
                        Exp.val(mapLastPart.getKey()), Exp.bin(bin.getBinName(), getBinType(basePath)), context);
            }
        } else {
            return null; // TODO
        }
    }

    private static CTX[] getContextArray(BasePath basePath, boolean includeLast) {
        // Nested (Context) map key access
        List<CTX> context = new ArrayList<>();

        for (int i = 0; i < basePath.getParts().size(); i++) {
            if (!includeLast && i == basePath.getParts().size() - 1) {
                continue;
            }
            AbstractPart part = basePath.getParts().get(i);
            switch (part.getPartType()) {
                case LIST_PART -> {
                    // TODO: support bin, index, rank, value
                }
                case MAP_PART -> {
                    // TODO: support other types (map rank, map index etc...)
                    context.add(CTX.mapKey(Value.get(((MapPart) part).getKey())));
                }
            }
        }
        return context.toArray(new CTX[0]);
    }

    private static Exp getExpVal(Exp.Type valueType, String listValue) {
        return switch (valueType) {
            case BOOL -> Exp.val(Boolean.parseBoolean(listValue));
            case INT -> Exp.val(Integer.parseInt(listValue));
            case STRING -> Exp.val(listValue);
            case FLOAT -> Exp.val(Float.parseFloat(listValue));
            default -> throw new IllegalStateException(
                    "Get by value from a List: unexpected value '%s'".formatted(valueType));
        };
    }

    private static Exp processSize(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType) {
        if (lastPathPart.getPartType() == PartType.LIST_PART) {
            ListPart list = (ListPart) lastPathPart;
            BinPart bin = basePath.getBinPart();
            return switch (list.getListPathType()) {
                case BIN -> ListExp.size(Exp.bin(bin.getBinName(), getBinType(basePath)));
                default -> throw new IllegalStateException(
                        "Get size from a List: unexpected value '%s'".formatted(valueType));
            };
        } else if (lastPathPart.getPartType() == PartType.MAP_PART) {
            BinPart bin = basePath.getBinPart();
            CTX[] context = getContextArray(basePath, true);
            // Valid Context (without synthetic map access for scenarios like mapBin1.size())
            if (context.length != 0 && !context[0].value.equals(Value.getAsNull())) {
                return MapExp.size(Exp.bin(bin.getBinName(), getBinType(basePath)), context);
            }
            return MapExp.size(Exp.bin(bin.getBinName(), getBinType(basePath)));
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
