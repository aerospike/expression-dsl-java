package com.aerospike.dsl.model;

import lombok.Getter;

@Getter
public class MapPart extends AbstractPart {

    private final int mapIndex;
    private final String mapValue;
    private final int mapRank;
    private final String mapKey;
    private final MapPathType mapPathType;

    public static Builder builder() {
        return new Builder();
    }

    private MapPart(Builder builder) {
        super(PartType.MAP_PART);
        this.mapIndex = builder.mapIndex;
        this.mapValue = builder.mapValue;
        this.mapRank = builder.mapRank;
        this.mapKey = builder.mapKey;
        this.mapPathType = builder.mapPathType;
    }

    public static class Builder {
        private int mapIndex;
        private String mapValue;
        private int mapRank;
        private String mapKey;
        private MapPathType mapPathType;

        public Builder() {
        }

        public Builder setMapIndex(int mapIndex) {
            this.mapIndex = mapIndex;
            this.mapPathType = MapPathType.INDEX;
            return this;
        }

        public Builder setMapValue(String mapValue) {
            this.mapValue = mapValue;
            this.mapPathType = MapPathType.VALUE;
            return this;
        }

        public Builder setMapRank(int mapRank) {
            this.mapRank = mapRank;
            this.mapPathType = MapPathType.RANK;
            return this;
        }

        public Builder setMapKey(String mapKey) {
            this.mapKey = mapKey;
            this.mapPathType = MapPathType.BIN;
            return this;
        }

        public MapPart build() {
            return new MapPart(this);
        }
    }

    public enum MapPathType {
        BIN,
        INDEX,
        VALUE,
        RANK
    }
}
