package com.aerospike.dsl.model.cdt.map;

import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.dsl.model.PathFunction;
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
        RANK_RANGE,
        RANK_RANGE_RELATIVE,
        INDEX_RANGE_RELATIVE
    }

    @Override
    public int getReturnType(PathFunction.ReturnParam returnParam) {
        return switch (returnParam) {
            case VALUE -> MapReturnType.VALUE;
            case KEY_VALUE -> MapReturnType.KEY_VALUE;
            case UNORDERED_MAP -> MapReturnType.UNORDERED_MAP;
            case ORDERED_MAP -> MapReturnType.ORDERED_MAP;
            case KEY -> MapReturnType.KEY;
            case INDEX -> MapReturnType.INDEX;
            case RANK -> MapReturnType.RANK;
            case COUNT, NONE -> MapReturnType.COUNT;
            case EXISTS -> MapReturnType.EXISTS;
            case REVERSE_INDEX -> MapReturnType.REVERSE_INDEX;
            case REVERSE_RANK -> MapReturnType.REVERSE_RANK;
        };
    }
}
