package com.aerospike.dsl.parts.cdt.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.cdt.MapReturnType;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.MapExp;
import com.aerospike.dsl.parts.path.BasePath;

import java.util.Optional;

import static com.aerospike.dsl.util.ParsingUtils.unquote;

public class MapKeyRange extends MapPart {
    private final boolean inverted;
    private final String start;
    private final String end;

    public MapKeyRange(boolean inverted, String start, String end) {
        super(MapPartType.KEY_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.end = end;
    }

    public static MapKeyRange from(ConditionParser.MapKeyRangeContext ctx) {
        ConditionParser.StandardMapKeyRangeContext keyRange = ctx.standardMapKeyRange();
        ConditionParser.InvertedMapKeyRangeContext invertedKeyRange = ctx.invertedMapKeyRange();

        if (keyRange != null || invertedKeyRange != null) {
            ConditionParser.KeyRangeIdentifierContext range =
                    keyRange != null ? keyRange.keyRangeIdentifier() : invertedKeyRange.keyRangeIdentifier();
            boolean isInverted = keyRange == null;

            String startKey = range.mapKey(0).NAME_IDENTIFIER() != null
                    ? range.mapKey(0).NAME_IDENTIFIER().getText()
                    : unquote(range.mapKey(0).QUOTED_STRING().getText());

            String endKey = Optional.ofNullable(range.mapKey(1))
                    .map(keyCtx -> keyCtx.NAME_IDENTIFIER() != null
                            ? keyCtx.NAME_IDENTIFIER().getText()
                            : unquote(keyCtx.QUOTED_STRING().getText()))
                    .orElse(null);

            return new MapKeyRange(isInverted, startKey, endKey);
        }
        throw new DslParseException("Could not translate MapKeyRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (inverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp startExp = Exp.val(start);
        Exp endExp = end != null ? Exp.val(end) : null;

        return MapExp.getByKeyRange(cdtReturnType, startExp, endExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }
}
