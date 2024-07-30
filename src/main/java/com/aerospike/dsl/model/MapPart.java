package com.aerospike.dsl.model;

import lombok.Getter;

@Getter
public class MapPart extends AbstractPart {

    private final String key;

    public MapPart(PartType partType, String key) {
        super(partType);
        this.key = key;
    }
}
