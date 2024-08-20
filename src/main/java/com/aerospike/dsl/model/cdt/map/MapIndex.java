package com.aerospike.dsl.model.cdt.map;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.model.BasePath;
import lombok.Getter;

@Getter
public class MapIndex extends MapPart {
    private final int index;

    public MapIndex(int index) {
        super(MapPartType.INDEX);
        this.index = index;
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return MapExp.getByIndex(cdtReturnType, valueType, Exp.val(getIndex()),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.mapIndex(getIndex());
    }

    public static MapIndex constructFromCTX(ConditionParser.MapIndexContext ctx) {
        return new MapIndex(Integer.parseInt(ctx.INT().getText()));
    }
}