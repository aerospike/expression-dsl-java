package com.aerospike.dsl.parts.cdt.map;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exceptions.ParseException;
import com.aerospike.dsl.parts.path.BasePath;

import static com.aerospike.dsl.util.ParsingUtils.unquote;

public class MapKey extends MapPart {
    private final String key;

    public MapKey(String key) {
        super(MapPartType.KEY);
        this.key = key;
    }

    public static MapKey from(ConditionParser.MapKeyContext ctx) {
        if (ctx.QUOTED_STRING() != null) {
            return new MapKey(unquote(ctx.QUOTED_STRING().getText()));
        }
        if (ctx.NAME_IDENTIFIER() != null) {
            return new MapKey(ctx.NAME_IDENTIFIER().getText());
        }
        throw new ParseException("Could not translate MapKey from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return MapExp.getByKey(cdtReturnType, valueType,
                Exp.val(key), Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.mapKey(Value.get(key));
    }
}
