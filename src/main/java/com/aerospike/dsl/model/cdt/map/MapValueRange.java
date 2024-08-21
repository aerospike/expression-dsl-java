package com.aerospike.dsl.model.cdt.map;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.BasePath;
import lombok.Getter;

@Getter
public class MapValueRange extends MapPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer end;

    public MapValueRange(boolean inverted, Integer start, Integer end) {
        super(MapPartType.VALUE_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.end = end;
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted()) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp start = Exp.val(getStart());
        Exp end = null;

        if (getEnd() != null) {
            end = Exp.val(getEnd());
        }
        return MapExp.getByValueRange(cdtReturnType, start, end, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }

    public static MapValueRange constructFromCTX(ConditionParser.MapValueRangeContext ctx) {
        ConditionParser.StandardMapValueRangeContext valueRange = ctx.standardMapValueRange();
        ConditionParser.InvertedMapValueRangeContext invertedValueRange = ctx.invertedMapValueRange();

        if (valueRange != null || invertedValueRange != null) {
            ConditionParser.ValueRangeIdentifierContext range =
                    valueRange != null ? valueRange.valueRangeIdentifier() : invertedValueRange.valueRangeIdentifier();
            boolean isInverted = valueRange == null;

            Integer startValue = Integer.parseInt(range.valueIdentifier(0).INT().getText());

            Integer endValue = null;

            if (range.valueIdentifier(1) != null) {
                if (range.valueIdentifier(1).INT() != null) {
                    endValue = Integer.parseInt(range.valueIdentifier(1).INT().getText());
                }
            }

            return new MapValueRange(isInverted, startValue, endValue);
        }
        throw new AerospikeDSLException("Could not translate MapValueRange from ctx: %s".formatted(ctx));
    }
}
