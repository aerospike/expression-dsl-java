package com.aerospike.dsl.index;

import com.aerospike.client.query.IndexType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * This class represents a secondary index created in the cluster.
 */
@Builder
@RequiredArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Getter
public class Index {

    private final String namespace;
    private final String bin;
    private final IndexType indexType;
    private int binValuesRatio = Integer.MIN_VALUE;
}
