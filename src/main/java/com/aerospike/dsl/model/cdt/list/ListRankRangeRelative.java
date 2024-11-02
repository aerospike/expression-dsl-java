package com.aerospike.dsl.model.cdt.list;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.BasePath;
import com.aerospike.dsl.util.ParsingUtils;

import static com.aerospike.dsl.util.ParsingUtils.subtractNullable;

public class ListRankRangeRelative extends ListPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer count;
    private final Object relative;

    public ListRankRangeRelative(boolean inverted, Integer start, Integer end, Object relative) {
        super(ListPartType.RANK_RANGE_RELATIVE);
        this.inverted = inverted;
        this.start = start;
        this.count = subtractNullable(end, start);
        this.relative = relative;
    }

    public static ListRankRangeRelative constructFromCTX(ConditionParser.ListRankRangeRelativeContext ctx) {
        ConditionParser.StandardListRankRangeRelativeContext rankRangeRelative = ctx.standardListRankRangeRelative();
        ConditionParser.InvertedListRankRangeRelativeContext invertedRankRangeRelative = ctx.invertedListRankRangeRelative();

        if (rankRangeRelative != null || invertedRankRangeRelative != null) {
            ConditionParser.RankRangeRelativeIdentifierContext range =
                    rankRangeRelative != null ? rankRangeRelative.rankRangeRelativeIdentifier()
                            : invertedRankRangeRelative.rankRangeRelativeIdentifier();
            boolean isInverted = rankRangeRelative == null;

            Integer start = Integer.parseInt(range.start().INT().getText());
            Integer end = null;
            if (range.relativeRankEnd().end() != null) {
                end = Integer.parseInt(range.relativeRankEnd().end().INT().getText());
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

            return new ListRankRangeRelative(isInverted, start, end, relativeValue);
        }
        throw new AerospikeDSLException("Could not translate ListRankRangeRelative from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (inverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        Exp relativeExp;
        if (relative instanceof String rel) {
            relativeExp = Exp.val(rel);
        } else if (relative instanceof Integer rel) {
            relativeExp = Exp.val(rel);
        } else {
            throw new AerospikeDSLException("Unsupported value relative rank");
        }

        Exp startExp = Exp.val(start);
        if (count == null) {
            return ListExp.getByValueRelativeRankRange(cdtReturnType, startExp, relativeExp,
                    Exp.bin(basePath.getBinPart().getBinName(),
                            basePath.getBinType()), context);
        }
        return ListExp.getByValueRelativeRankRange(cdtReturnType, startExp, relativeExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
