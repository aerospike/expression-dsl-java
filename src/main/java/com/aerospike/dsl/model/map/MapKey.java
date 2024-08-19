package com.aerospike.dsl.model.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.util.ParsingUtils;
import lombok.Getter;

@Getter
public class MapKey extends MapPart {
    private final String key;

    public MapKey(String key) {
        super(MapPartType.KEY);
        this.key = key;
    }

    public static MapKey constructFromCTX(ConditionParser.MapKeyContext ctx) {
        if (ctx.QUOTED_STRING() != null) {
            return new MapKey(ParsingUtils.getWithoutQuotes(ctx.QUOTED_STRING().getText()));
        }
        if (ctx.NAME_IDENTIFIER() != null) {
            return new MapKey(ctx.NAME_IDENTIFIER().getText());
        }
        throw new AerospikeDSLException("Could not translate MapKey from ctx: %s".formatted(ctx));
    }
}
