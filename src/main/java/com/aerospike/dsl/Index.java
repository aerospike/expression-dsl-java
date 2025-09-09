package com.aerospike.dsl;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * This class represents a secondary index created in the cluster.
 */
@Builder
@EqualsAndHashCode
@Getter
public class Index {

    /**
     * Namespace of the indexed bin
     */
    private final String namespace;
    /**
     * Name of the indexed bin
     */
    private final String bin;
    /**
     * {@link IndexType} of the index
     */
    private final IndexType indexType;
    /**
     * Cardinality of the index calculated using "sindex-stat" command and looking at the ratio of entries
     * to unique bin values for the given secondary index on the node (entries_per_bval)
     */
    private int binValuesRatio;
    /**
     * {@link IndexCollectionType} of the index
     */
    private IndexCollectionType indexCollectionType;
    /**
     * Array of {@link CTX} representing context of the index
     */
    private CTX[] ctx;
}
