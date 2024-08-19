package com.aerospike.dsl.model.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import lombok.Getter;

@Getter
public class MapRankRange extends MapPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer count;

    public MapRankRange(boolean inverted, Integer start, Integer count) {
        super(MapPartType.RANK_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.count = count;
    }

    public static MapRankRange constructFromCTX(ConditionParser.MapRankRangeContext ctx) {
        ConditionParser.StandardMapRankRangeContext rankRange = ctx.standardMapRankRange();
        ConditionParser.InvertedMapRankRangeContext invertedRankRange = ctx.invertedMapRankRange();

        if (rankRange != null || invertedRankRange != null) {
            ConditionParser.RankRangeIdentifierContext range =
                    rankRange != null ? rankRange.rankRangeIdentifier() : invertedRankRange.rankRangeIdentifier();
            boolean isInverted = rankRange == null;

            Integer start = Integer.parseInt(range.start().INT().getText());
            Integer count = null;
            if (range.count() != null) {
                count = Integer.parseInt(range.count().INT().getText());
            }

            return new MapRankRange(isInverted, start, count);
        }
        throw new AerospikeDSLException("Could not translate MapRankRange from ctx: %s".formatted(ctx));
    }
}
