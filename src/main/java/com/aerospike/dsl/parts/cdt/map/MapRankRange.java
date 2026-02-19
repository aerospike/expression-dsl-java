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

public class MapRankRange extends MapPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer count;

    public MapRankRange(boolean isInverted, Integer start, Integer end) {
        super(MapPartType.RANK_RANGE);
        this.isInverted = isInverted;
        this.start = start;
        this.count = subtractNullable(end, start);
    }

    public static MapRankRange from(ConditionParser.MapRankRangeContext ctx) {
        ConditionParser.StandardMapRankRangeContext rankRange = ctx.standardMapRankRange();
        ConditionParser.InvertedMapRankRangeContext invertedRankRange = ctx.invertedMapRankRange();

        if (rankRange != null || invertedRankRange != null) {
            ConditionParser.RankRangeIdentifierContext range =
                    rankRange != null ? rankRange.rankRangeIdentifier() : invertedRankRange.rankRangeIdentifier();
            boolean isInverted = rankRange == null;

            Integer start = parseSignedInt(range.start().signedInt());
            Integer end = null;
            if (range.end() != null) {
                end = parseSignedInt(range.end().signedInt());
            }

            return new MapRankRange(isInverted, start, end);
        }
        throw new DslParseException("Could not translate MapRankRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp startExp = Exp.val(start);
        if (count == null) {
            return MapExp.getByRankRange(cdtReturnType, startExp, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        }

        return MapExp.getByRankRange(cdtReturnType, startExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
