package com.aerospike.dsl.model.list;

import com.aerospike.dsl.ConditionParser;
import lombok.Getter;

@Getter
public class ListRank extends ListPart {
    private final int rank;

    public ListRank(int rank) {
        super(ListPartType.RANK);
        this.rank = rank;
    }

    public static ListRank constructFromCTX(ConditionParser.ListRankContext ctx) {
        return new ListRank(Integer.parseInt(ctx.INT().getText()));
    }
}
