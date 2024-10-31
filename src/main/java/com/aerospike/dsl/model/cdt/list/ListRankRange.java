package com.aerospike.dsl.model.cdt.list;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.BasePath;
import lombok.Getter;

import static com.aerospike.dsl.util.ParsingUtils.subtractNullable;

@Getter
public class ListRankRange extends ListPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer count;

    public ListRankRange(boolean inverted, Integer start, Integer end) {
        super(ListPartType.RANK_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.count = subtractNullable(end, start);
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted()) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }
        Exp start = Exp.val(getStart());
        Exp count = null;
        if (getCount() != null) {
            count = Exp.val(getCount());
        }
        if (count == null) {
            return ListExp.getByRankRange(cdtReturnType, start, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        } else {
            return ListExp.getByRankRange(cdtReturnType, start, count, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        }
    }

    public static ListRankRange constructFromCTX(ConditionParser.ListRankRangeContext ctx) {
        ConditionParser.StandardListRankRangeContext rankRange = ctx.standardListRankRange();
        ConditionParser.InvertedListRankRangeContext invertedRankRange = ctx.invertedListRankRange();

        if (rankRange != null || invertedRankRange != null) {
            ConditionParser.RankRangeIdentifierContext range =
                    rankRange != null ? rankRange.rankRangeIdentifier() : invertedRankRange.rankRangeIdentifier();
            boolean isInverted = rankRange == null;

            Integer start = Integer.parseInt(range.start().INT().getText());
            Integer end = null;
            if (range.end() != null) {
                end = Integer.parseInt(range.end().INT().getText());
            }

            return new ListRankRange(isInverted, start, end);
        }
        throw new AerospikeDSLException("Could not translate ListRankRange from ctx: %s".formatted(ctx));
    }
}
