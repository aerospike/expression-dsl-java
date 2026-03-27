package com.aerospike.dsl.parts.cdt.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.client.Value;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.MapExp;
import com.aerospike.dsl.parts.path.BasePath;

import static com.aerospike.dsl.util.ParsingUtils.parseValueIdentifier;

public class MapValue extends MapPart {
    private final Object value;

    public MapValue(Object value) {
        super(MapPartType.VALUE);
        this.value = value;
    }

    public static MapValue from(ConditionParser.MapValueContext ctx) {
        return new MapValue(parseValueIdentifier(ctx.valueIdentifier()));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return MapExp.getByValue(cdtReturnType, valueToExp(), Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }

    private Exp valueToExp() {
        if (value instanceof Boolean b) return Exp.val(b);
        if (value instanceof String s) return Exp.val(s);
        if (value instanceof Float f) return Exp.val(f);
        return Exp.val((Integer) value);
    }

    @Override
    public CTX getContext() {
        return CTX.mapValue(Value.get(value));
    }
}
