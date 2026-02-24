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

public class MapRankRangeRelative extends MapPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer count;
    private final Object relative;

    public MapRankRangeRelative(boolean isInverted, Integer start, Integer end, Object relative) {
        super(MapPartType.RANK_RANGE_RELATIVE);
        this.isInverted = isInverted;
        this.start = start;
        this.count = subtractNullable(end, start);
        this.relative = relative;
    }

    public static MapRankRangeRelative from(ConditionParser.MapRankRangeRelativeContext ctx) {
        ConditionParser.StandardMapRankRangeRelativeContext rankRangeRelative = ctx.standardMapRankRangeRelative();
        ConditionParser.InvertedMapRankRangeRelativeContext invertedRankRangeRelative = ctx.invertedMapRankRangeRelative();

        if (rankRangeRelative != null || invertedRankRangeRelative != null) {
            ConditionParser.RankRangeRelativeIdentifierContext range =
                    rankRangeRelative != null ? rankRangeRelative.rankRangeRelativeIdentifier()
                            : invertedRankRangeRelative.rankRangeRelativeIdentifier();
            boolean isInverted = rankRangeRelative == null;

            Integer start = parseSignedInt(range.start().signedInt());
            Integer end = null;
            if (range.relativeRankEnd().end() != null) {
                end = parseSignedInt(range.relativeRankEnd().end().signedInt());
            }

            Object relativeValue = null;
            if (range.relativeRankEnd().relativeValue() != null) {
                ConditionParser.ValueIdentifierContext valueIdentifierContext
                        = range.relativeRankEnd().relativeValue().valueIdentifier();
                if (valueIdentifierContext.signedInt() != null) {
                    relativeValue = parseSignedInt(valueIdentifierContext.signedInt());
                } else if (valueIdentifierContext.NAME_IDENTIFIER() != null) {
                    relativeValue = valueIdentifierContext.NAME_IDENTIFIER().getText();
                } else if (valueIdentifierContext.QUOTED_STRING() != null) {
                    relativeValue = unquote(valueIdentifierContext.QUOTED_STRING().getText());
                }
            }

            return new MapRankRangeRelative(isInverted, start, end, relativeValue);
        }
        throw new DslParseException("Could not translate MapRankRangeRelative from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp relativeExp;
        if (relative instanceof String rel) {
            relativeExp = Exp.val(rel);
        } else if (relative instanceof Integer rel) {
            relativeExp = Exp.val(rel);
        } else {
            throw new DslParseException("Unsupported value relative rank");
        }

        Exp startExp = Exp.val(start);
        if (count == null) {
            return MapExp.getByValueRelativeRankRange(cdtReturnType, startExp, relativeExp,
                    Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
        }

        return MapExp.getByValueRelativeRankRange(cdtReturnType, startExp, relativeExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }
}
