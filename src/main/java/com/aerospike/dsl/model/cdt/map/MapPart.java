package com.aerospike.dsl.model.cdt.map;

import com.aerospike.dsl.model.cdt.CdtPart;
import lombok.Getter;

@Getter
public abstract class MapPart extends CdtPart {

    private final MapPartType mapPartType;

    public MapPart(MapPartType mapPartType) {
        super(PartType.MAP_PART);
        this.mapPartType = mapPartType;
    }

    public enum MapPartType {
        BIN,
        KEY,
        INDEX,
        VALUE,
        RANK,
        KEY_RANGE,
        KEY_LIST,
        INDEX_RANGE,
        VALUE_LIST,
        VALUE_RANGE,
        RANK_RANGE
    }
}
