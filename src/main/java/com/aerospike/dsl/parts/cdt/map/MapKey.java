package com.aerospike.dsl.parts.cdt.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.client.Value;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.MapExp;
import com.aerospike.dsl.parts.path.BasePath;
import com.aerospike.dsl.util.ParsingUtils;

public class MapKey extends MapPart {
    private final String key;

    public MapKey(String key) {
        super(MapPartType.KEY);
        this.key = key;
    }

    public static MapKey from(ConditionParser.MapKeyContext ctx) {
        return new MapKey(ParsingUtils.parseMapKey(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return MapExp.getByKey(cdtReturnType, valueType,
                Exp.val(key), Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.mapKey(Value.get(key));
    }
}
