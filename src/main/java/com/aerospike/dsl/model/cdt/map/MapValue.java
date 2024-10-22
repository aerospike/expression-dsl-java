package com.aerospike.dsl.model.cdt.map;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.model.BasePath;
import com.aerospike.dsl.util.ParsingUtils;
import lombok.Getter;

@Getter
public class MapValue extends MapPart {
    private final Object value;

    public MapValue(Object value) {
        super(MapPartType.VALUE);
        this.value = value;
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        Exp value = switch (valueType) {
            case BOOL -> Exp.val((Boolean) getValue());
            case STRING -> Exp.val((String) getValue());
            case FLOAT -> Exp.val((Float) getValue());
            default -> Exp.val((Integer) getValue()); // for getByValue the default is INT
        };
        return MapExp.getByValue(cdtReturnType, value, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.mapValue(Value.get(getValue()));
    }

    public static MapValue constructFromCTX(ConditionParser.MapValueContext ctx) {
        Object mapValue = null;
        if (ctx.valueIdentifier().NAME_IDENTIFIER() != null) {
            mapValue = ctx.valueIdentifier().NAME_IDENTIFIER().getText();
        } else if (ctx.valueIdentifier().QUOTED_STRING() != null) {
            mapValue = ParsingUtils.getWithoutQuotes(ctx.valueIdentifier().QUOTED_STRING().getText());
        } else if (ctx.valueIdentifier().INT() != null) {
            mapValue = Integer.parseInt(ctx.valueIdentifier().INT().getText());
        }
        return new MapValue(mapValue);
    }
}
