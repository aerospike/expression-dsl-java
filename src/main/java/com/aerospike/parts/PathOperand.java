package com.aerospike.parts;

import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import lombok.Getter;

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

        AbstractPart lastPathPart = basePath.getParts().getLast();
        switch (pathFunctionType) {
            case GET -> {
                if (lastPathPart.getType() == Type.LIST_PART) {
                    ListPart list = (ListPart) lastPathPart;
                    BinPart bin = basePath.getBinOperand();
                    return switch (list.getListPathType()) {
                        case BIN -> Exp.listBin(bin.getBinName());
                        case INDEX ->
                                ListExp.getByIndex(listReturnType, valueType, Exp.val(list.getListIndex()), Exp.bin(bin.getBinName(), getBinType(basePath)));
                        case VALUE ->
                                ListExp.getByValue(listReturnType, Exp.val(list.getListIndex()), Exp.bin(bin.getBinName(), getBinType(basePath)));
                        case RANK ->
                                ListExp.getByRank(listReturnType, valueType, Exp.val(list.getListIndex()), Exp.bin(bin.getBinName(), getBinType(basePath)));
                    };
                }
            }
            case SIZE -> {
                if (lastPathPart.getType() == Type.LIST_PART) {
                    ListPart list = (ListPart) lastPathPart;
                    BinPart bin = basePath.getBinOperand();
                    return switch (list.getListPathType()) {
                        case BIN -> ListExp.size(Exp.bin(bin.getBinName(), getBinType(basePath)));
                        default -> null;
                    };
                }
            }
//            case COUNT -> {
//            }
        }
        return null; // TODO
    }

    private static Exp.Type getBinType(BasePath basePath) {
        if (basePath.getParts().getFirst().getType() == Type.LIST_PART) {
            return Exp.Type.LIST;
        }
        return null; // TODO
    }
}
