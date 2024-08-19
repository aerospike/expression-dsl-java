package com.aerospike.dsl.model.cdt.map;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.model.BasePath;
import lombok.Getter;

@Getter
public class MapRank extends MapPart {
    private final int rank;

    public MapRank(int rank) {
        super(MapPartType.RANK);
        this.rank = rank;
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return MapExp.getByRank(cdtReturnType, valueType, Exp.val(getRank()),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    public static MapRank constructFromCTX(ConditionParser.MapRankContext ctx) {
        return new MapRank(Integer.parseInt(ctx.INT().getText()));
    }
}
