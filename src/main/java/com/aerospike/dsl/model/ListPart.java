package com.aerospike.dsl.model;

import lombok.Getter;

@Getter
public class ListPart extends AbstractPart {

    private final int listIndex;
    private final String listValue;
    private final int listRank;
    private final boolean listBin;
    private final ListPartType listPartType;

    private ListPart(Builder builder) {
        super(PartType.LIST_PART);
        this.listIndex = builder.listIndex;
        this.listValue = builder.listValue;
        this.listRank = builder.listRank;
        this.listBin = builder.listBin;
        this.listPartType = builder.listPartType;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int listIndex;
        private String listValue;
        private int listRank;
        private boolean listBin;
        private ListPartType listPartType;

        public Builder() {
        }

        public Builder setListIndex(int listIndex) {
            this.listIndex = listIndex;
            this.listPartType = ListPartType.INDEX;
            return this;
        }

        public Builder setListValue(String listValue) {
            this.listValue = listValue;
            this.listPartType = ListPartType.VALUE;
            return this;
        }

        public Builder setListRank(int listRank) {
            this.listRank = listRank;
            this.listPartType = ListPartType.RANK;
            return this;
        }

        public Builder setListBin(boolean listBin) {
            this.listBin = listBin;
            this.listPartType = ListPartType.BIN;
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
