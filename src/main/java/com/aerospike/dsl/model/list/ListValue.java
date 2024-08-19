package com.aerospike.dsl.model.list;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.util.ParsingUtils;
import lombok.Getter;

@Getter
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
}
