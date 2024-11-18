package com.aerospike.dsl.model.cdt.map;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.model.BasePath;

import static com.aerospike.dsl.util.ParsingUtils.unquote;

public class MapValue extends MapPart {
    private final Object value;

    public MapValue(Object value) {
        super(MapPartType.VALUE);
        this.value = value;
    }

    public static MapValue from(ConditionParser.MapValueContext ctx) {
        Object mapValue = null;
        if (ctx.valueIdentifier().NAME_IDENTIFIER() != null) {
            mapValue = ctx.valueIdentifier().NAME_IDENTIFIER().getText();
        } else if (ctx.valueIdentifier().QUOTED_STRING() != null) {
            mapValue = unquote(ctx.valueIdentifier().QUOTED_STRING().getText());
        } else if (ctx.valueIdentifier().INT() != null) {
            mapValue = Integer.parseInt(ctx.valueIdentifier().INT().getText());
        }
        return new MapValue(mapValue);
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
