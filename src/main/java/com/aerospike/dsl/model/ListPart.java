package com.aerospike.dsl.model;

import lombok.Getter;

@Getter
public class ListPart extends AbstractPart {

    private final ListPartType listPartType;
    private final int listIndex;
    private final Object listValue;
    private final int listRank;
    private final boolean listBin;

    private ListPart(Builder builder) {
        super(PartType.LIST_PART);
        this.listPartType = builder.listPartType;
        this.listIndex = builder.listIndex;
        this.listValue = builder.listValue;
        this.listRank = builder.listRank;
        this.listBin = builder.listBin;
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

        public ListPart build() {
            return new ListPart(this);
        }
    }

    public enum ListPartType {
        BIN,
        INDEX,
        VALUE,
        RANK
    }
}
