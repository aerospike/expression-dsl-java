package com.aerospike.dsl.model;

import lombok.Getter;

@Getter
public class MapPart extends AbstractPart {

    private final String key;

    public MapPart(String key) {
        super(PartType.MAP_PART);
        this.key = key;
    }
}
