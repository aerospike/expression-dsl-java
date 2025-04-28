package com.aerospike.dsl.part.cdt.map;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.part.path.BasePath;

public class MapRank extends MapPart {
    private final int rank;

    public MapRank(int rank) {
        super(MapPartType.RANK);
        this.rank = rank;
    }

    public static MapRank from(ConditionParser.MapRankContext ctx) {
        return new MapRank(Integer.parseInt(ctx.INT().getText()));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return MapExp.getByRank(cdtReturnType, valueType, Exp.val(rank),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.mapRank(rank);
    }
}
