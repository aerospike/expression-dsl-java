package com.aerospike.dsl.model.map;

import lombok.Getter;

@Getter
public class MapKeyBin extends MapPart {
    private final String key;

    public MapKeyBin(String key) {
        super(MapPart.MapPartType.BIN);
        this.key = key;
    }
}
