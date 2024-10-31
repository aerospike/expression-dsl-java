package com.aerospike.dsl.model.cdt.map;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.BasePath;
import com.aerospike.dsl.util.ParsingUtils;
import lombok.Getter;

import static com.aerospike.dsl.util.ParsingUtils.subtractNullable;

@Getter
public class MapIndexRangeRelative extends MapPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer count;
    private final String relative;

    public MapIndexRangeRelative(boolean inverted, Integer start, Integer end, String relative) {
        super(MapPartType.INDEX_RANGE_RELATIVE);
        this.inverted = inverted;
        this.start = start;
        this.count = subtractNullable(end, start);
        this.relative = relative;
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted()) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }
        Exp keyExp = Exp.val(getRelative());
        Exp start = Exp.val(getStart());
        Exp count = null;
        if (getCount() != null) {
            count = Exp.val(getCount());
        }
        if (count == null) {
            return MapExp.getByKeyRelativeIndexRange(cdtReturnType, keyExp, start, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        } else {
            return MapExp.getByKeyRelativeIndexRange(cdtReturnType, keyExp, start, count, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        }
    }

    public static MapIndexRangeRelative constructFromCTX(ConditionParser.MapIndexRangeRelativeContext ctx) {
        ConditionParser.StandardMapIndexRangeRelativeContext indexRangeRelative = ctx.standardMapIndexRangeRelative();
        ConditionParser.InvertedMapIndexRangeRelativeContext invertedIndexRangeRelative = ctx.invertedMapIndexRangeRelative();

        if (indexRangeRelative != null || invertedIndexRangeRelative != null) {
            ConditionParser.IndexRangeRelativeIdentifierContext range =
                    indexRangeRelative != null ? indexRangeRelative.indexRangeRelativeIdentifier()
                            : invertedIndexRangeRelative.indexRangeRelativeIdentifier();
            boolean isInverted = indexRangeRelative == null;

            Integer start = Integer.parseInt(range.start().INT().getText());
            Integer end = null;
            String relativeKey = null;
            if (range.relativeKeyEnd().end() != null) {
                end = Integer.parseInt(range.relativeKeyEnd().end().INT().getText());
            }

            if (range.relativeKeyEnd().mapKey() != null) {
                ConditionParser.MapKeyContext mapKeyContext = range.relativeKeyEnd().mapKey();
                if (mapKeyContext.NAME_IDENTIFIER() != null) {
                    relativeKey = mapKeyContext.NAME_IDENTIFIER().getText();
                } else if (mapKeyContext.QUOTED_STRING() != null) {
                    relativeKey = ParsingUtils.getWithoutQuotes(mapKeyContext.QUOTED_STRING().getText());
                }
            }
            return new MapIndexRangeRelative(isInverted, start, end, relativeKey);
        }
        throw new AerospikeDSLException("Could not translate MapIndexRangeRelative from ctx: %s".formatted(ctx));
    }
}
