package com.aerospike.dsl.parts.cdt.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.MapExp;
import com.aerospike.dsl.parts.path.BasePath;

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
