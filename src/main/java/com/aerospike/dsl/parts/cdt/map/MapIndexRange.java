package com.aerospike.dsl.parts.cdt.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.cdt.MapReturnType;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.MapExp;
import com.aerospike.dsl.parts.path.BasePath;

import static com.aerospike.dsl.util.ParsingUtils.subtractNullable;

public class MapIndexRange extends MapPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer count;

    public MapIndexRange(boolean inverted, Integer start, Integer end) {
        super(MapPartType.INDEX_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.count = subtractNullable(end, start);
    }

    public static MapIndexRange from(ConditionParser.MapIndexRangeContext ctx) {
        ConditionParser.StandardMapIndexRangeContext indexRange = ctx.standardMapIndexRange();
        ConditionParser.InvertedMapIndexRangeContext invertedIndexRange = ctx.invertedMapIndexRange();

        if (indexRange != null || invertedIndexRange != null) {
            ConditionParser.IndexRangeIdentifierContext range =
                    indexRange != null ? indexRange.indexRangeIdentifier() : invertedIndexRange.indexRangeIdentifier();
            boolean isInverted = indexRange == null;

            Integer start = Integer.parseInt(range.start().INT().getText());
            Integer end = null;
            if (range.end() != null) {
                end = Integer.parseInt(range.end().INT().getText());
            }

            return new MapIndexRange(isInverted, start, end);
        }
        throw new DslParseException("Could not translate MapIndexRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (inverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp startExp = Exp.val(start);
        if (count == null) {
            return MapExp.getByIndexRange(cdtReturnType, startExp, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        }

        return MapExp.getByIndexRange(cdtReturnType, startExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
