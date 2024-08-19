package com.aerospike.dsl.model.map;

import com.aerospike.dsl.model.AbstractPart;
import lombok.Getter;

import java.util.List;

@Getter
public class MapPart extends AbstractPart {

    private final MapPartType mapPartType;
    private final int mapIndex;
    private final Object mapValue;
    private final int mapRank;
    private final String mapKey;
    private final MapKeyRange mapKeyRange;
    private final MapKeyList mapKeyList;
    private final MapIndexRange mapIndexRange;
    private final MapValueList mapValueList;
    private final MapValueRange mapValueRange;
    private final MapRankRange mapRankRange;

    public static Builder builder() {
        return new Builder();
    }

    private MapPart(Builder builder) {
        super(PartType.MAP_PART);
        this.mapPartType = builder.mapPartType;
        this.mapIndex = builder.mapIndex;
        this.mapValue = builder.mapValue;
        this.mapRank = builder.mapRank;
        this.mapKey = builder.mapKey;
        this.mapKeyRange = builder.mapKeyRange;
        this.mapKeyList = builder.mapKeyList;
        this.mapIndexRange = builder.mapIndexRange;
        this.mapValueList = builder.mapValueList;
        this.mapValueRange = builder.mapValueRange;
        this.mapRankRange = builder.mapRankRange;
    }

    public static class Builder {
        private MapPartType mapPartType;
        private int mapIndex;
        private Object mapValue;
        private int mapRank;
        private String mapKey;
        private MapKeyRange mapKeyRange;
        private MapKeyList mapKeyList;
        private MapIndexRange mapIndexRange;
        private MapValueList mapValueList;
        private MapValueRange mapValueRange;
        private MapRankRange mapRankRange;

        public Builder() {
        }

        public Builder setMapKeyBin(String mapKeyBin) {
            this.mapPartType = MapPartType.BIN;
            this.mapKey = mapKeyBin;
            return this;
        }

        public Builder setMapIndex(int mapIndex) {
            this.mapPartType = MapPartType.INDEX;
            this.mapIndex = mapIndex;
            return this;
        }

        public Builder setMapValue(Object mapValue) {
            this.mapPartType = MapPartType.VALUE;
            this.mapValue = mapValue;
            return this;
        }

        public Builder setMapRank(int mapRank) {
            this.mapPartType = MapPartType.RANK;
            this.mapRank = mapRank;
            return this;
        }

        public Builder setMapKey(String mapKey) {
            this.mapPartType = MapPartType.KEY;
            this.mapKey = mapKey;
            return this;
        }

        public Builder setMapKeyRange(boolean inverted, String start, String end) {
            this.mapPartType = MapPartType.KEY_RANGE;
            this.mapKeyRange = new MapKeyRange(inverted, start, end);
            return this;
        }

        public Builder setMapKeyList(boolean inverted, List<String> keyList) {
            this.mapPartType = MapPartType.KEY_LIST;
            this.mapKeyList = new MapKeyList(inverted, keyList);
            return this;
        }

        public Builder setMapIndexRange(boolean inverted, Integer start, Integer count) {
            this.mapPartType = MapPartType.INDEX_RANGE;
            this.mapIndexRange = new MapIndexRange(inverted, start, count);
            return this;
        }

        public Builder setMapValueList(boolean inverted, List<?> valueList) {
            this.mapPartType = MapPartType.VALUE_LIST;
            this.mapValueList = new MapValueList(inverted, valueList);
            return this;
        }

        public Builder setMapValueRange(boolean inverted, Integer start, Integer end) {
            this.mapPartType = MapPartType.VALUE_RANGE;
            this.mapValueRange = new MapValueRange(inverted, start, end);
            return this;
        }

        public Builder setMapRankRange(boolean inverted, Integer start, Integer count) {
            this.mapPartType = MapPartType.RANK_RANGE;
            this.mapRankRange = new MapRankRange(inverted, start, count);
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
        KEY_LIST,
        INDEX_RANGE,
        VALUE_LIST,
        VALUE_RANGE,
        RANK_RANGE
    }
}
