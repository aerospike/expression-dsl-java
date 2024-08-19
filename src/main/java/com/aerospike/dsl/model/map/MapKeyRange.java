package com.aerospike.dsl.model.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.util.ParsingUtils;
import lombok.Getter;

@Getter
public class MapKeyRange extends MapPart {
    private final boolean inverted;
    private final String start;
    private final String end;

    public MapKeyRange(boolean inverted, String start, String end) {
        super(MapPartType.KEY_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.end = end;
    }

    public static MapKeyRange constructFromCTX(ConditionParser.MapKeyRangeContext ctx) {
        ConditionParser.StandardMapKeyRangeContext keyRange = ctx.standardMapKeyRange();
        ConditionParser.InvertedMapKeyRangeContext invertedKeyRange = ctx.invertedMapKeyRange();

        if (keyRange != null || invertedKeyRange != null) {
            ConditionParser.KeyRangeIdentifierContext range =
                    keyRange != null ? keyRange.keyRangeIdentifier() : invertedKeyRange.keyRangeIdentifier();
            boolean isInverted = keyRange == null;

            String startKey = range.mapKey(0).NAME_IDENTIFIER() != null
                    ? range.mapKey(0).NAME_IDENTIFIER().getText()
                    : ParsingUtils.getWithoutQuotes(range.mapKey(0).QUOTED_STRING().getText());

            String endKey = range.mapKey(1) != null
                    ? (range.mapKey(1).NAME_IDENTIFIER() != null
                    ? range.mapKey(1).NAME_IDENTIFIER().getText()
                    : ParsingUtils.getWithoutQuotes(range.mapKey(1).QUOTED_STRING().getText()))
                    : null;

            return new MapKeyRange(isInverted, startKey, endKey);
        }
        throw new AerospikeDSLException("Could not translate MapKeyRange from ctx: %s".formatted(ctx));
    }
}
