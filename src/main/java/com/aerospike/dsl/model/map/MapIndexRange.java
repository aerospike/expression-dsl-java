package com.aerospike.dsl.model.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import lombok.Getter;

@Getter
public class MapIndexRange extends MapPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer count;

    public MapIndexRange(boolean inverted, Integer start, Integer count) {
        super(MapPartType.INDEX_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.count = count;
    }

    public static MapIndexRange constructFromCTX(ConditionParser.MapIndexRangeContext ctx) {
        ConditionParser.StandardMapIndexRangeContext indexRange = ctx.standardMapIndexRange();
        ConditionParser.InvertedMapIndexRangeContext invertedIndexRange = ctx.invertedMapIndexRange();

        if (indexRange != null || invertedIndexRange != null) {
            ConditionParser.IndexRangeIdentifierContext range =
                    indexRange != null ? indexRange.indexRangeIdentifier() : invertedIndexRange.indexRangeIdentifier();
            boolean isInverted = indexRange == null;

            Integer start = Integer.parseInt(range.start().INT().getText());
            Integer count = null;
            if (range.count() != null) {
                count = Integer.parseInt(range.count().INT().getText());
            }

            return new MapIndexRange(isInverted, start, count);
        }
        throw new AerospikeDSLException("Could not translate MapIndexRange from ctx: %s".formatted(ctx));
    }
}
