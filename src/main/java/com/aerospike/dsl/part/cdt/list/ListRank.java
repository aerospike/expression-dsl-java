package com.aerospike.dsl.part.cdt.list;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.part.path.BasePath;

public class ListRank extends ListPart {
    private final int rank;

    public ListRank(int rank) {
        super(ListPartType.RANK);
        this.rank = rank;
    }

    public static ListRank from(ConditionParser.ListRankContext ctx) {
        return new ListRank(Integer.parseInt(ctx.INT().getText()));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return ListExp.getByRank(cdtReturnType, valueType, Exp.val(rank),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.listRank(rank);
    }
}
