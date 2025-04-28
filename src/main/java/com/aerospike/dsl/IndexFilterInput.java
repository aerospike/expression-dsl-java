package com.aerospike.dsl;

import com.aerospike.client.query.Filter;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Collection;

/**
 * This class stores namespace and indexes required to build secondary index {@link Filter}.
 */
@AllArgsConstructor(staticName = "of")
@Getter
public class IndexFilterInput {

    /**
     * Namespace to be used for creating {@link Filter}. Is matched with namespace of indexes
     */
    private String namespace;
    /**
     * Collection of {@link Index} objects to be used for creating {@link Filter}.
     * Namespace of indexes is matched with the given {@link #namespace}, bin name and index type are matched
     * with bins in DSL String
     */
    private Collection<Index> indexes;
}
