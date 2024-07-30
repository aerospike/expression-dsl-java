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
        if (!parts.isEmpty()) { // if there is a path inside bin given
            AbstractPart lastPathPart = parts.get(parts.size() - 1);
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
            return switch (list.getListPathType()) {
                case BIN -> Exp.listBin(bin.getBinName());
                case INDEX -> ListExp.getByIndex(listReturnType, valueType, Exp.val(list.getListIndex()),
                        Exp.bin(bin.getBinName(), getBinType(basePath)));
                case VALUE -> {
                    Exp value = getExpVal(valueType, list.getListValue());
                    yield ListExp.getByValue(listReturnType, value, Exp.bin(bin.getBinName(), getBinType(basePath)));
                }
                case RANK -> ListExp.getByRank(listReturnType, valueType, Exp.val(list.getListRank()),
                        Exp.bin(bin.getBinName(), getBinType(basePath)));
            };
        } else if (lastPathPart.getPartType() == PartType.MAP_PART) {
            MapPart mapLastPart = (MapPart) lastPathPart;
            BinPart bin = basePath.getBinPart();

            if (basePath.getParts().size() == 1) {
                // Single map key access
                return MapExp.getByKey(listReturnType, valueType,
                        Exp.val(mapLastPart.getKey()), Exp.mapBin(bin.getBinName()));
            } else {
                // Nested (Context) map key access
                List<CTX> context = new ArrayList<>();

                // No need to iterate the last part, it is not considered a CTX
                for (int i = 0; i < basePath.getParts().size() - 1; i++) {
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
                return MapExp.getByKey(listReturnType, valueType,
                        Exp.val(mapLastPart.getKey()), Exp.mapBin(bin.getBinName()), context.toArray(new CTX[0]));
            }
        } else {
            return null; // TODO
        }
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
        } else {
            return null; // TODO
        }
    }

    private static Exp.Type getBinType(BasePath basePath) {
        List<AbstractPart> parts = basePath.getParts();
        if (parts.get(parts.size() - 1).getPartType() == PartType.LIST_PART) {
            return Exp.Type.LIST;
        }
        return null; // TODO
    }
}
