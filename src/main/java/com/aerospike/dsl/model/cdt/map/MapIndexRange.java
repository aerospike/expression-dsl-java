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
public class MapIndexRange extends MapPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer count;

    public MapIndexRange(boolean inverted, Integer start, Integer end) {
        super(MapPartType.INDEX_RANGE);
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
            return MapExp.getByIndexRange(cdtReturnType, start, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        } else {
            return MapExp.getByIndexRange(cdtReturnType, start, count, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        }
    }

    public static MapIndexRange constructFromCTX(ConditionParser.MapIndexRangeContext ctx) {
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
        throw new AerospikeDSLException("Could not translate MapIndexRange from ctx: %s".formatted(ctx));
    }
}
