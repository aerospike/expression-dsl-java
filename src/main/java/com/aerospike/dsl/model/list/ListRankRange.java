package com.aerospike.dsl.model.list;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import lombok.Getter;

@Getter
public class ListRankRange extends ListPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer count;

    public ListRankRange(boolean inverted, Integer start, Integer count) {
        super(ListPartType.RANK_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.count = count;
    }

    public static ListRankRange constructFromCTX(ConditionParser.ListRankRangeContext ctx) {
        ConditionParser.StandardListRankRangeContext rankRange = ctx.standardListRankRange();
        ConditionParser.InvertedListRankRangeContext invertedRankRange = ctx.invertedListRankRange();

        if (rankRange != null || invertedRankRange != null) {
            ConditionParser.RankRangeIdentifierContext range =
                    rankRange != null ? rankRange.rankRangeIdentifier() : invertedRankRange.rankRangeIdentifier();
            boolean isInverted = rankRange == null;

            Integer start = Integer.parseInt(range.start().INT().getText());
            Integer count = null;
            if (range.count() != null) {
                count = Integer.parseInt(range.count().INT().getText());
            }

            return new ListRankRange(isInverted, start, count);
        }
        throw new AerospikeDSLException("Could not translate ListRankRange from ctx: %s".formatted(ctx));
    }
}
