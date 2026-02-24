package com.aerospike.dsl.parts.cdt.list;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.ListExp;
import com.aerospike.dsl.parts.path.BasePath;

import static com.aerospike.dsl.util.ParsingUtils.parseSignedInt;

public class ListRank extends ListPart {
    private final int rank;

    public ListRank(int rank) {
        super(ListPartType.RANK);
        this.rank = rank;
    }

    public static ListRank from(ConditionParser.ListRankContext ctx) {
        return new ListRank(parseSignedInt(ctx.signedInt()));
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
