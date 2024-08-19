package com.aerospike.dsl.model.cdt.list;

import com.aerospike.dsl.model.cdt.CdtPart;
import lombok.Getter;

@Getter
public abstract class ListPart extends CdtPart {

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
