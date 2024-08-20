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

@Getter
public class MapRankRangeRelative extends MapPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer count;
    private final Object relative;

    public MapRankRangeRelative(boolean inverted, Integer start, Integer count, Object relative) {
        super(MapPartType.RANK_RANGE_RELATIVE);
        this.inverted = inverted;
        this.start = start;
        this.count = count;
        this.relative = relative;
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted()) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }
        Exp relativeExp;
        if (getRelative() instanceof String) {
            relativeExp = Exp.val((String) getRelative());
        } else if (getRelative() instanceof Integer) {
            relativeExp = Exp.val((Integer) getRelative());
        } else {
            throw new AerospikeDSLException("Unsupported value relative rank");
        }
        Exp start = Exp.val(getStart());
        Exp count = null;
        if (getCount() != null) {
            count = Exp.val(getCount());
        }
        if (count == null) {
            return MapExp.getByValueRelativeRankRange(cdtReturnType, start, relativeExp,
                    Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
        } else {
            return MapExp.getByValueRelativeRankRange(cdtReturnType, start, relativeExp, count,
                    Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
        }
    }

    public static MapRankRangeRelative constructFromCTX(ConditionParser.MapRankRangeRelativeContext ctx) {
        ConditionParser.StandardMapRankRangeRelativeContext rankRangeRelative = ctx.standardMapRankRangeRelative();
        ConditionParser.InvertedMapRankRangeRelativeContext invertedRankRangeRelative = ctx.invertedMapRankRangeRelative();

        if (rankRangeRelative != null || invertedRankRangeRelative != null) {
            ConditionParser.RankRangeRelativeIdentifierContext range =
                    rankRangeRelative != null ? rankRangeRelative.rankRangeRelativeIdentifier()
                            : invertedRankRangeRelative.rankRangeRelativeIdentifier();
            boolean isInverted = rankRangeRelative == null;

            Integer start = Integer.parseInt(range.start().INT().getText());
            Integer count = null;

            if (range.relativeRankEnd().count() != null) {
                count = Integer.parseInt(range.relativeRankEnd().count().INT().getText());
            }

            Object relativeValue = null;

            if (range.relativeRankEnd().relativeValue() != null) {
                ConditionParser.ValueIdentifierContext valueIdentifierContext
                        = range.relativeRankEnd().relativeValue().valueIdentifier();
                if (valueIdentifierContext.INT() != null) {
                    relativeValue = Integer.parseInt(valueIdentifierContext.INT().getText());
                } else if (valueIdentifierContext.NAME_IDENTIFIER() != null) {
                    relativeValue = valueIdentifierContext.NAME_IDENTIFIER().getText();
                } else if (valueIdentifierContext.QUOTED_STRING() != null) {
                    relativeValue = ParsingUtils.getWithoutQuotes(valueIdentifierContext.QUOTED_STRING().getText());
                }
            }

            return new MapRankRangeRelative(isInverted, start, count, relativeValue);
        }
        throw new AerospikeDSLException("Could not translate MapRankRangeRelative from ctx: %s".formatted(ctx));
    }
}
