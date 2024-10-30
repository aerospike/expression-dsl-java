package com.aerospike.dsl.model.cdt.map;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.BasePath;
import lombok.Getter;

import static com.aerospike.dsl.util.ParsingUtils.subtractOrReturnNull;

@Getter
public class MapRankRange extends MapPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer count;

    public MapRankRange(boolean inverted, Integer start, Integer end) {
        super(MapPartType.RANK_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.count = subtractOrReturnNull(end, start);
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted()) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }
        Exp start = Exp.val(getStart());
        Exp count = null;
        if (getCount() != null) {
            count = Exp.val(getCount());
        }
        if (count == null) {
            return MapExp.getByRankRange(cdtReturnType, start, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        } else {
            return MapExp.getByRankRange(cdtReturnType, start, count, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        }
    }

    public static MapRankRange constructFromCTX(ConditionParser.MapRankRangeContext ctx) {
        ConditionParser.StandardMapRankRangeContext rankRange = ctx.standardMapRankRange();
        ConditionParser.InvertedMapRankRangeContext invertedRankRange = ctx.invertedMapRankRange();

        if (rankRange != null || invertedRankRange != null) {
            ConditionParser.RankRangeIdentifierContext range =
                    rankRange != null ? rankRange.rankRangeIdentifier() : invertedRankRange.rankRangeIdentifier();
            boolean isInverted = rankRange == null;

            Integer start = Integer.parseInt(range.start().INT().getText());
            Integer end = null;
            if (range.end() != null) {
                end = Integer.parseInt(range.end().INT().getText());
            }

            return new MapRankRange(isInverted, start, end);
        }
        throw new AerospikeDSLException("Could not translate MapRankRange from ctx: %s".formatted(ctx));
    }
}
