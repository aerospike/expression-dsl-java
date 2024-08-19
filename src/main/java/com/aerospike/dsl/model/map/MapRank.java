package com.aerospike.dsl.model.map;

import com.aerospike.dsl.ConditionParser;
import lombok.Getter;

@Getter
public class MapRank extends MapPart {
    private final int rank;

    public MapRank(int rank) {
        super(MapPartType.RANK);
        this.rank = rank;
    }

    public static MapRank constructFromCTX(ConditionParser.MapRankContext ctx) {
        return new MapRank(Integer.parseInt(ctx.INT().getText()));
    }
}
