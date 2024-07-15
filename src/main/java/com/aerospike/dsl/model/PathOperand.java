package com.aerospike.dsl.model;

import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import lombok.Getter;

import java.util.List;

@Getter
public class PathOperand extends AbstractPart {

    public PathOperand(Exp exp) {
        super(Type.PATH_OPERAND, exp);
    }

    public static Exp processPath(BasePath basePath, PathFunction pathFunction) {
        PathFunction.TYPE_PARAM typeParam = PathFunction.TYPE_PARAM.INT;
        PathFunction.RETURN_PARAM returnParam = PathFunction.RETURN_PARAM.VALUE;
        PathFunction.PATH_FUNCTION_TYPE pathFunctionType = PathFunction.PATH_FUNCTION_TYPE.GET;

        if (pathFunction != null) {
            if (pathFunction.getReturnParam() != null) returnParam = pathFunction.getReturnParam();
            if (pathFunction.getTypeParam() != null) typeParam = pathFunction.getTypeParam();
            if (pathFunction.getPathFunctionType() != null) pathFunctionType = pathFunction.getPathFunctionType();

        }

        Exp.Type valueType = Exp.Type.valueOf(typeParam.toString());

        int listReturnType = switch (returnParam) {
            case VALUE -> ListReturnType.VALUE;
            case COUNT, NONE -> ListReturnType.COUNT;
        };

        List<AbstractPart> parts = basePath.getParts();
        if (!parts.isEmpty()) { // if there is a path inside bin given
            AbstractPart lastPathPart = parts.get(parts.size() - 1);
            return switch (pathFunctionType) {
                case GET, COUNT -> processGet(basePath, lastPathPart, valueType, listReturnType);
                case SIZE -> processSize(basePath, lastPathPart, valueType);
            };
        }
        throw new IllegalStateException("Expecting other parts of path except bin");
    }

    private static Exp processGet(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType, int listReturnType) {
        if (lastPathPart.getType() == Type.LIST_PART) {
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
                    String.format("Get by value from a List: unexpected value '%s'", valueType));
        };
    }

    private static Exp processSize(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType) {
        if (lastPathPart.getType() == Type.LIST_PART) {
            ListPart list = (ListPart) lastPathPart;
            BinPart bin = basePath.getBinPart();
            return switch (list.getListPathType()) {
                case BIN -> ListExp.size(Exp.bin(bin.getBinName(), getBinType(basePath)));
                default -> throw new IllegalStateException(
                        String.format("Get size from a List: unexpected value '%s'", valueType));
            };
        } else {
            return null; // TODO
        }
    }

    private static Exp.Type getBinType(BasePath basePath) {
        List<AbstractPart> parts = basePath.getParts();
        if (parts.get(parts.size() - 1).getType() == Type.LIST_PART) {
            return Exp.Type.LIST;
        }
        return null; // TODO
    }
}
