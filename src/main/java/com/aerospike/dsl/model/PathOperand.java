package com.aerospike.dsl.model;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.cdt.CdtPart;
import com.aerospike.dsl.model.cdt.list.ListIndex;
import com.aerospike.dsl.model.cdt.list.ListPart;
import com.aerospike.dsl.model.cdt.list.ListRank;
import com.aerospike.dsl.model.cdt.list.ListValue;
import com.aerospike.dsl.model.cdt.map.MapIndex;
import com.aerospike.dsl.model.cdt.map.MapKey;
import com.aerospike.dsl.model.cdt.map.MapKeyBin;
import com.aerospike.dsl.model.cdt.map.MapPart;
import com.aerospike.dsl.model.cdt.map.MapRank;
import com.aerospike.dsl.model.cdt.map.MapValue;
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
                lastPathPart = new MapKeyBin(basePath.getBinPart().getBinName());
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
        return ((CdtPart) lastPathPart).constructExp(basePath, valueType, cdtReturnType, context);
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
                        case INDEX -> context.add(CTX.listIndex(((ListIndex) listPart).getIndex()));
                        case VALUE -> context.add(CTX.listValue(Value.get(((ListValue) listPart).getValue())));
                        case RANK -> context.add(CTX.listRank(((ListRank) listPart).getRank()));
                        default -> throw new AerospikeDSLException("Unsupported List Part in Context: %s."
                                .formatted(listPart.getListPartType()));
                    }
                }
                case MAP_PART -> {
                    MapPart mapPart = (MapPart) part;
                    switch (mapPart.getMapPartType()) {
                        case KEY -> context.add(CTX.mapKey(Value.get(((MapKey) mapPart).getKey())));
                        case INDEX -> context.add(CTX.mapIndex(((MapIndex) mapPart).getIndex()));
                        case VALUE -> context.add(CTX.mapValue(Value.get(((MapValue) mapPart).getValue())));
                        case RANK -> context.add(CTX.mapRank(((MapRank) mapPart).getRank()));
                    }
                }
            }
        }
        return context.toArray(new CTX[0]);
    }

    private static Exp processSize(BasePath basePath, AbstractPart lastPathPart) {
        BinPart bin = basePath.getBinPart();
        if (lastPathPart.getPartType() == PartType.LIST_PART) {
            ListPart list = (ListPart) lastPathPart;
            if (list.getListPartType().equals(ListPart.ListPartType.BIN)) {
                return ListExp.size(Exp.bin(bin.getBinName(), basePath.getBinType()));
            } else {
                // In size() the last element is considered context
                CTX[] context = getContextArray(basePath, true);
                return ListExp.size(Exp.bin(bin.getBinName(), basePath.getBinType()), context);
            }
        } else if (lastPathPart.getPartType() == PartType.MAP_PART) {
            MapPart map = (MapPart) lastPathPart;
            if (map.getMapPartType().equals(MapPart.MapPartType.BIN)) {
                return MapExp.size(Exp.bin(bin.getBinName(), basePath.getBinType()));
            } else {
                // In size() the last element is considered context
                CTX[] context = getContextArray(basePath, true);
                return MapExp.size(Exp.bin(bin.getBinName(), basePath.getBinType()), context);
            }
        } else {
            return null;
        }
    }
}
