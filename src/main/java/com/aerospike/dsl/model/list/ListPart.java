package com.aerospike.dsl.model.list;

import com.aerospike.dsl.model.AbstractPart;
import lombok.Getter;

@Getter
public class ListPart extends AbstractPart {

    private final ListPartType listPartType;

    public ListPart(ListPartType listPartType) {
        super(PartType.LIST_PART);
        this.listPartType = listPartType;
    }

    public enum ListPartType {
        BIN,
        INDEX,
        VALUE,
        RANK,
        INDEX_RANGE,
        VALUE_LIST,
        VALUE_RANGE,
        RANK_RANGE
    }
}
