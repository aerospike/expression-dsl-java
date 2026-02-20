package com.aerospike.dsl;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Collection;
import java.util.List;

/**
 * This class stores namespace and indexes required to build secondary index Filter
 */
@AllArgsConstructor(staticName = "of")
@Getter
public class IndexContext {

    /**
     * Namespace to be used for creating secondary index Filter. Is matched with namespace of indexes
     */
    private String namespace;
    /**
     * Collection of {@link Index} objects to be used for creating secondary index Filter.
     * Namespace of indexes is matched with the given {@link #namespace}, bin name and index type are matched
     * with bins in DSL String
     */
    private Collection<Index> indexes;


    /**
     * Create index context specifying the index to be used
     *
     * @param namespace  Namespace to be used for creating {@link com.aerospike.dsl.client.query.Filter}.
     *                   Is matched with namespace of indexes
     * @param indexes    Collection of {@link Index} objects to be used for creating
     *                   {@link com.aerospike.dsl.client.query.Filter}. Bin name and
     *                   index type are matched with bins in DSL String
     * @param indexToUse The name of an index to use for creating
     *                   {@link com.aerospike.dsl.client.query.Filter}. If the index with the specified name
     *                   is not found (or {@code indexToUse} is {@code null}), the resulting index is chosen
     *                   the usual way (cardinality-based or alphabetically)
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext of(String namespace, Collection<Index> indexes, String indexToUse) {
        List<Index> matchingIndexes = indexes.stream().filter(idx -> areNamesEqual(idx, indexToUse)).toList();
        return new IndexContext(namespace, matchingIndexes.isEmpty() ? indexes : matchingIndexes);
    }

    private static boolean areNamesEqual(Index idx, String indexToUse) {
        return idx != null && idx.getName() != null && idx.getName().equals(indexToUse);
    }
}
