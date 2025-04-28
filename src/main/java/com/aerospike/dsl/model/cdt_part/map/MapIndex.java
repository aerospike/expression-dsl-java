package com.aerospike.dsl.model.cdt_part.map;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.model.path.BasePath;

public class MapIndex extends MapPart {
    private final int index;

    public MapIndex(int index) {
        super(MapPartType.INDEX);
        this.index = index;
    }

    public static MapIndex from(ConditionParser.MapIndexContext ctx) {
        return new MapIndex(Integer.parseInt(ctx.INT().getText()));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return MapExp.getByIndex(cdtReturnType, valueType, Exp.val(index),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.mapIndex(index);
    }
}
