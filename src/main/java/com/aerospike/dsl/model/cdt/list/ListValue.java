package com.aerospike.dsl.model.cdt.list;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.model.BasePath;
import com.aerospike.dsl.util.ParsingUtils;
import lombok.Getter;

@Getter
public class ListValue extends ListPart {
    private final Object value;

    public ListValue(Object value) {
        super(ListPartType.VALUE);
        this.value = value;
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        Exp value = getExpVal(valueType, getValue());
        return ListExp.getByValue(cdtReturnType, value, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
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
}
