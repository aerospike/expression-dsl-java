package com.aerospike.dsl.model.cdt.list;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.model.BasePath;
import lombok.Getter;

@Getter
public class ListRank extends ListPart {
    private final int rank;

    public ListRank(int rank) {
        super(ListPartType.RANK);
        this.rank = rank;
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return ListExp.getByRank(cdtReturnType, valueType, Exp.val(getRank()),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    public static ListRank constructFromCTX(ConditionParser.ListRankContext ctx) {
        return new ListRank(Integer.parseInt(ctx.INT().getText()));
    }
}
