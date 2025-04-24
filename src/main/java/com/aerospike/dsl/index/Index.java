package com.aerospike.dsl.index;

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

    private final String namespace;
    private final String bin;
    private final IndexType indexType;
    private int binValuesRatio;
}
