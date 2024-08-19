package com.aerospike.dsl.model.list;

import com.aerospike.dsl.ConditionParser;
import lombok.Getter;

@Getter
public class ListIndex extends ListPart {
    private final int index;

    public ListIndex(int index) {
        super(ListPartType.INDEX);
        this.index = index;
    }

    public static ListIndex constructFromCTX(ConditionParser.ListIndexContext ctx) {
        return new ListIndex(Integer.parseInt(ctx.INT().getText()));
    }
}
