package com.aerospike.dsl.model.list;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import lombok.Getter;

@Getter
public class ListIndexRange extends ListPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer count;

    public ListIndexRange(boolean inverted, Integer start, Integer count) {
        super(ListPartType.INDEX_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.count = count;
    }

    public static ListIndexRange constructFromCTX(ConditionParser.ListIndexRangeContext ctx) {
        ConditionParser.StandardListIndexRangeContext indexRange = ctx.standardListIndexRange();
        ConditionParser.InvertedListIndexRangeContext invertedIndexRange = ctx.invertedListIndexRange();

        if (indexRange != null || invertedIndexRange != null) {
            ConditionParser.IndexRangeIdentifierContext range =
                    indexRange != null ? indexRange.indexRangeIdentifier() : invertedIndexRange.indexRangeIdentifier();
            boolean isInverted = indexRange == null;

            Integer start = Integer.parseInt(range.start().INT().getText());
            Integer count = null;
            if (range.count() != null) {
                count = Integer.parseInt(range.count().INT().getText());
            }

            return new ListIndexRange(isInverted, start, count);
        }
        throw new AerospikeDSLException("Could not translate ListIndexRange from ctx: %s".formatted(ctx));
    }
}
