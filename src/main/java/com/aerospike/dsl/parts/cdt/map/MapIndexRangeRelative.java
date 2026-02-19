package com.aerospike.dsl.parts.cdt.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.cdt.MapReturnType;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.MapExp;
import com.aerospike.dsl.parts.path.BasePath;

import static com.aerospike.dsl.util.ParsingUtils.parseSignedInt;
import static com.aerospike.dsl.util.ParsingUtils.subtractNullable;
import static com.aerospike.dsl.util.ParsingUtils.unquote;

public class MapIndexRangeRelative extends MapPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer count;
    private final String relative;

    public MapIndexRangeRelative(boolean isInverted, Integer start, Integer end, String relative) {
        super(MapPartType.INDEX_RANGE_RELATIVE);
        this.isInverted = isInverted;
        this.start = start;
        this.count = subtractNullable(end, start);
        this.relative = relative;
    }

    public static MapIndexRangeRelative from(ConditionParser.MapIndexRangeRelativeContext ctx) {
        ConditionParser.StandardMapIndexRangeRelativeContext indexRangeRelative = ctx.standardMapIndexRangeRelative();
        ConditionParser.InvertedMapIndexRangeRelativeContext invertedIndexRangeRelative = ctx.invertedMapIndexRangeRelative();

        if (indexRangeRelative != null || invertedIndexRangeRelative != null) {
            ConditionParser.IndexRangeRelativeIdentifierContext range =
                    indexRangeRelative != null ? indexRangeRelative.indexRangeRelativeIdentifier()
                            : invertedIndexRangeRelative.indexRangeRelativeIdentifier();
            boolean isInverted = indexRangeRelative == null;

            Integer start = parseSignedInt(range.start().signedInt());
            Integer end = null;
            if (range.relativeKeyEnd().end() != null) {
                end = parseSignedInt(range.relativeKeyEnd().end().signedInt());
            }

            String relativeKey = null;
            if (range.relativeKeyEnd().mapKey() != null) {
                ConditionParser.MapKeyContext mapKeyContext = range.relativeKeyEnd().mapKey();
                if (mapKeyContext.NAME_IDENTIFIER() != null) {
                    relativeKey = mapKeyContext.NAME_IDENTIFIER().getText();
                } else if (mapKeyContext.QUOTED_STRING() != null) {
                    relativeKey = unquote(mapKeyContext.QUOTED_STRING().getText());
                }
            }
            return new MapIndexRangeRelative(isInverted, start, end, relativeKey);
        }
        throw new DslParseException("Could not translate MapIndexRangeRelative from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp keyExp = Exp.val(relative);
        Exp startExp = Exp.val(start);
        if (count == null) {
            return MapExp.getByKeyRelativeIndexRange(cdtReturnType, keyExp, startExp,
                    Exp.bin(basePath.getBinPart().getBinName(),
                            basePath.getBinType()), context);
        }

        return MapExp.getByKeyRelativeIndexRange(cdtReturnType, keyExp, startExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
