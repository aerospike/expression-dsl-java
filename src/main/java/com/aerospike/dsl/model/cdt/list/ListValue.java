package com.aerospike.dsl.model.cdt.list;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.model.BasePath;
import com.aerospike.dsl.util.ParsingUtils;

public class ListValue extends ListPart {
    private final Object value;

    public ListValue(Object value) {
        super(ListPartType.VALUE);
        this.value = value;
    }

    public static ListValue constructFromCTX(ConditionParser.ListValueContext ctx) {
        Object listValue = null;
        if (ctx.valueIdentifier().NAME_IDENTIFIER() != null) {
            listValue = ctx.valueIdentifier().NAME_IDENTIFIER().getText();
        } else if (ctx.valueIdentifier().QUOTED_STRING() != null) {
            listValue = ParsingUtils.getWithoutQuotes(ctx.valueIdentifier().QUOTED_STRING().getText());
        } else if (ctx.valueIdentifier().INT() != null) {
            listValue = Integer.parseInt(ctx.valueIdentifier().INT().getText());
        }
        return new ListValue(listValue);
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        Exp valueExp = switch (valueType) {
            case BOOL -> Exp.val((Boolean) value);
            case STRING -> Exp.val((String) value);
            case FLOAT -> Exp.val((Float) value);
            default -> Exp.val((Integer) value); // for getByValue the default is INT
        };
        return ListExp.getByValue(cdtReturnType, valueExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.listValue(Value.get(value));
    }
}
