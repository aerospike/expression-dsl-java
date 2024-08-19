package com.aerospike.dsl.model.map;

import com.aerospike.dsl.ConditionParser;
import lombok.Getter;

@Getter
public class MapIndex extends MapPart {
    private final int index;

    public MapIndex(int index) {
        super(MapPartType.INDEX);
        this.index = index;
    }

    public static MapIndex constructFromCTX(ConditionParser.MapIndexContext ctx) {
        return new MapIndex(Integer.parseInt(ctx.INT().getText()));
    }
}
