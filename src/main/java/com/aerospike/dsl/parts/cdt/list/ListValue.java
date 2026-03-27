package com.aerospike.dsl.parts.cdt.list;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.client.Value;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.ListExp;
import com.aerospike.dsl.parts.path.BasePath;

import static com.aerospike.dsl.util.ParsingUtils.parseValueIdentifier;

public class ListValue extends ListPart {
    private final Object value;

    public ListValue(Object value) {
        super(ListPartType.VALUE);
        this.value = value;
    }

    public static ListValue from(ConditionParser.ListValueContext ctx) {
        return new ListValue(parseValueIdentifier(ctx.valueIdentifier()));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return ListExp.getByValue(cdtReturnType, valueToExp(), Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }

    private Exp valueToExp() {
        if (value instanceof Boolean b) return Exp.val(b);
        if (value instanceof String s) return Exp.val(s);
        if (value instanceof Float f) return Exp.val(f);
        return Exp.val((Integer) value);
    }

    @Override
    public CTX getContext() {
        return CTX.listValue(Value.get(value));
    }
}
