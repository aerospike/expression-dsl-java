package com.aerospike.dsl.model.cdt.list;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.model.BasePath;
import lombok.Getter;

@Getter
public class ListIndex extends ListPart {
    private final int index;

    public ListIndex(int index) {
        super(ListPartType.INDEX);
        this.index = index;
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return ListExp.getByIndex(cdtReturnType, valueType, Exp.val(getIndex()),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    public static ListIndex constructFromCTX(ConditionParser.ListIndexContext ctx) {
        return new ListIndex(Integer.parseInt(ctx.INT().getText()));
    }
}
