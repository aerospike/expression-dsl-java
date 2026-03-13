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
        Exp valueExp = switch (valueType) {
            case BOOL -> Exp.val((Boolean) value);
            case STRING -> Exp.val((String) value);
            case FLOAT -> Exp.val((Float) value);
            default -> Exp.val((Integer) value); // for getByValue the default is INT
        };

        return MapExp.getByValue(cdtReturnType, valueExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.mapValue(Value.get(value));
    }
}
