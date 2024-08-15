package com.aerospike.dsl.model;

import lombok.Getter;

import java.util.List;

@Getter
public class MapPart extends AbstractPart {

    private final int mapIndex;
    private final String mapValue;
    private final int mapRank;
    private final String mapKey;
    private final MapKeyRange mapKeyRange;
    private final MapKeyList mapKeyList;
    private final MapPartType mapPartType;

    public static Builder builder() {
        return new Builder();
    }

    private MapPart(Builder builder) {
        super(PartType.MAP_PART);
        this.mapIndex = builder.mapIndex;
        this.mapValue = builder.mapValue;
        this.mapRank = builder.mapRank;
        this.mapKey = builder.mapKey;
        this.mapKeyRange = builder.mapKeyRange;
        this.mapKeyList = builder.mapKeyList;
        this.mapPartType = builder.mapPartType;
    }

    public static class Builder {
        private int mapIndex;
        private String mapValue;
        private int mapRank;
        private String mapKey;
        private MapKeyRange mapKeyRange;
        private MapKeyList mapKeyList;
        private MapPartType mapPartType;

        public Builder() {
        }

        public Builder setMapKeyBin(String mapKeyBin) {
            this.mapKey = mapKeyBin;
            this.mapPartType = MapPartType.BIN;
            return this;
        }

        public Builder setMapIndex(int mapIndex) {
            this.mapIndex = mapIndex;
            this.mapPartType = MapPartType.INDEX;
            return this;
        }

        public Builder setMapValue(String mapValue) {
            this.mapValue = mapValue;
            this.mapPartType = MapPartType.VALUE;
            return this;
        }

        public Builder setMapRank(int mapRank) {
            this.mapRank = mapRank;
            this.mapPartType = MapPartType.RANK;
            return this;
        }

        public Builder setMapKey(String mapKey) {
            this.mapKey = mapKey;
            this.mapPartType = MapPartType.KEY;
            return this;
        }

        public Builder setMapKeyRange(boolean inverted, String start, String end) {
            this.mapKeyRange = new MapKeyRange(inverted, start, end);
            this.mapPartType = MapPartType.KEY_RANGE;
            return this;
        }

        public Builder setMapKeyList(boolean inverted, List<String> keyList) {
            this.mapKeyList = new MapKeyList(inverted, keyList);
            this.mapPartType = MapPartType.KEY_LIST;
            return this;
        }

        public MapPart build() {
            return new MapPart(this);
        }
    }

    public enum MapPartType {
        BIN,
        KEY,
        INDEX,
        VALUE,
        RANK,
        KEY_RANGE,
        KEY_LIST
    }
}
