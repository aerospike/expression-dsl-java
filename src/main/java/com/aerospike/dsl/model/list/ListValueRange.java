package com.aerospike.dsl.model.list;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import lombok.Getter;

@Getter
public class ListValueRange extends ListPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer end;

    public ListValueRange(boolean inverted, Integer start, Integer end) {
        super(ListPartType.VALUE_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.end = end;
    }

    public static ListValueRange constructFromCTX(ConditionParser.ListValueRangeContext ctx) {
        ConditionParser.StandardListValueRangeContext valueRange = ctx.standardListValueRange();
        ConditionParser.InvertedListValueRangeContext invertedValueRange = ctx.invertedListValueRange();

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

            return new ListValueRange(isInverted, startValue, endValue);
        }
        throw new AerospikeDSLException("Could not translate ListValueRange from ctx: %s".formatted(ctx));
    }
}
