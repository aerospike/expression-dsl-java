package com.aerospike.dsl.model.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.util.ParsingUtils;
import lombok.Getter;

@Getter
public class MapValue extends MapPart {
    private final Object value;

    public MapValue(Object value) {
        super(MapPartType.VALUE);
        this.value = value;
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
