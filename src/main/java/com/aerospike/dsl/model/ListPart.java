package com.aerospike.dsl.model;

import lombok.Getter;

import java.util.List;

@Getter
public class ListPart extends AbstractPart {

    private final ListPartType listPartType;
    private final int listIndex;
    private final Object listValue;
    private final int listRank;
    private final boolean listBin;
    private final ListIndexRange listIndexRange;
    private final ListValueList listValueList;
    private final ListValueRange listValueRange;

    private ListPart(Builder builder) {
        super(PartType.LIST_PART);
        this.listPartType = builder.listPartType;
        this.listIndex = builder.listIndex;
        this.listValue = builder.listValue;
        this.listRank = builder.listRank;
        this.listBin = builder.listBin;
        this.listIndexRange = builder.listIndexRange;
        this.listValueList = builder.listValueList;
        this.listValueRange = builder.listValueRange;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ListPartType listPartType;
        private int listIndex;
        private Object listValue;
        private int listRank;
        private boolean listBin;
        private ListIndexRange listIndexRange;
        private ListValueList listValueList;
        private ListValueRange listValueRange;

        public Builder() {
        }

        public Builder setListIndex(int listIndex) {
            this.listPartType = ListPartType.INDEX;
            this.listIndex = listIndex;
            return this;
        }

        public Builder setListValue(Object listValue) {
            this.listPartType = ListPartType.VALUE;
            this.listValue = listValue;
            return this;
        }

        public Builder setListRank(int listRank) {
            this.listPartType = ListPartType.RANK;
            this.listRank = listRank;
            return this;
        }

        public Builder setListBin(boolean listBin) {
            this.listPartType = ListPartType.BIN;
            this.listBin = listBin;
            return this;
        }

        public Builder setListIndexRange(boolean inverted, Integer start, Integer count) {
            this.listPartType = ListPartType.INDEX_RANGE;
            this.listIndexRange = new ListIndexRange(inverted, start, count);
            return this;
        }

        public Builder setListValueList(boolean inverted, List<?> valueList) {
            this.listPartType = ListPartType.VALUE_LIST;
            this.listValueList = new ListValueList(inverted, valueList);
            return this;
        }

        public Builder setListValueRange(boolean inverted, Integer start, Integer end) {
            this.listPartType = ListPartType.VALUE_RANGE;
            this.listValueRange = new ListValueRange(inverted, start, end);
            return this;
        }

        public ListPart build() {
            return new ListPart(this);
        }
    }

    public enum ListPartType {
        BIN,
        INDEX,
        VALUE,
        RANK,
        INDEX_RANGE,
        VALUE_LIST,
        VALUE_RANGE
    }
}
