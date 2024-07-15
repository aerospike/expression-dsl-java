package com.aerospike.dsl.model;

import lombok.Getter;

@Getter
public class ListPart extends AbstractPart {

    private final int listIndex;
    private final String listValue;
    private final int listRank;
    private final boolean listBin;
    private final ListPathType listPathType;

    private ListPart(Builder builder) {
        super(Type.LIST_PART);
        this.listIndex = builder.listIndex;
        this.listValue = builder.listValue;
        this.listRank = builder.listRank;
        this.listBin = builder.listBin;
        this.listPathType = builder.listPathType;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int listIndex;
        private String listValue;
        private int listRank;
        private boolean listBin;
        private ListPathType listPathType;

        public Builder() {
        }

        public Builder setListIndex(int listIndex) {
            this.listIndex = listIndex;
            this.listPathType = ListPathType.INDEX;
            return this;
        }

        public Builder setListValue(String listValue) {
            this.listValue = listValue;
            this.listPathType = ListPathType.VALUE;
            return this;
        }

        public Builder setListRank(int listRank) {
            this.listRank = listRank;
            this.listPathType = ListPathType.RANK;
            return this;
        }

        public Builder setListBin(boolean listBin) {
            this.listBin = listBin;
            this.listPathType = ListPathType.BIN;
            return this;
        }

        public ListPart build() {
            return new ListPart(this);
        }
    }

    public enum ListPathType {
        BIN,
        INDEX,
        VALUE,
        RANK
    }
}
