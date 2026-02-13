package com.aerospike.dsl.parts.cdt.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.MapExp;
import com.aerospike.dsl.parts.path.BasePath;

import static com.aerospike.dsl.util.ParsingUtils.parseSignedInt;

public class MapIndex extends MapPart {
    private final int index;

    public MapIndex(int index) {
        super(MapPartType.INDEX);
        this.index = index;
    }

    public static MapIndex from(ConditionParser.MapIndexContext ctx) {
        return new MapIndex(parseSignedInt(ctx.signedInt()));
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
