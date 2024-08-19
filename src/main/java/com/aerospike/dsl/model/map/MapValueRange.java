package com.aerospike.dsl.model.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import lombok.Getter;

@Getter
public class MapValueRange extends MapPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer end;

    public MapValueRange(boolean inverted, Integer start, Integer end) {
        super(MapPartType.VALUE_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.end = end;
    }

    public static MapValueRange constructFromCTX(ConditionParser.MapValueRangeContext ctx) {
        ConditionParser.StandardMapValueRangeContext valueRange = ctx.standardMapValueRange();
        ConditionParser.InvertedMapValueRangeContext invertedValueRange = ctx.invertedMapValueRange();

        if (valueRange != null || invertedValueRange != null) {
            ConditionParser.ValueRangeIdentifierContext range =
                    valueRange != null ? valueRange.valueRangeIdentifier() : invertedValueRange.valueRangeIdentifier();
            boolean isInverted = valueRange == null;

            Integer startValue = Integer.parseInt(range.valueIdentifier(0).INT().getText());

            Integer endValue = null;

            if (range.valueIdentifier(1) != null) {
                if (range.valueIdentifier(1).INT() != null) {
                    endValue = Integer.parseInt(range.valueIdentifier(1).INT().getText());
                }
            }

            return new MapValueRange(isInverted, startValue, endValue);
        }
        throw new AerospikeDSLException("Could not translate MapValueRange from ctx: %s".formatted(ctx));
    }
}
