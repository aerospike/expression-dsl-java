package com.aerospike.dsl;

import lombok.Getter;

import java.util.Collection;
import java.util.List;

/**
 * This class stores namespace and indexes required to build secondary index Filter
 */
@Getter
public class IndexContext {

    /**
     * Namespace to be used for creating secondary index Filter. Is matched with namespace of indexes
     */
    private final String namespace;
    /**
     * Collection of {@link Index} objects to be used for creating secondary index Filter.
     * Namespace of indexes is matched with the given {@link #namespace}, bin name and index type are matched
     * with bins in DSL String
     */
    private final Collection<Index> indexes;

    private IndexContext(String namespace, Collection<Index> indexes) {
        this.namespace = namespace;
        this.indexes = indexes;
    }

    /**
     * Create index context with namespace and indexes.
     *
     * @param namespace Namespace to be used for creating {@link com.aerospike.dsl.client.query.Filter}.
     *                  Must not be null or blank
     * @param indexes   Collection of {@link Index} objects to be used for creating Filter
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext of(String namespace, Collection<Index> indexes) {
        validateNamespace(namespace);
        return new IndexContext(namespace, indexes);
    }

    /**
     * Create index context specifying the index to be used
     *
     * @param namespace  Namespace to be used for creating {@link com.aerospike.dsl.client.query.Filter}.
     *                   Must not be null or blank
     * @param indexes    Collection of {@link Index} objects to be used for creating Filter
     * @param indexToUse The name of an index to use. If not found or null, index is chosen by cardinality or alphabetically
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext of(String namespace, Collection<Index> indexes, String indexToUse) {
        validateNamespace(namespace);
        List<Index> matchingIndexes = indexes.stream()
                .filter(idx -> indexMatches(idx, namespace, indexToUse))
                .toList();
        return new IndexContext(namespace, matchingIndexes.isEmpty() ? indexes : matchingIndexes);
    }

    private static void validateNamespace(String namespace) {
        if (namespace == null) {
            throw new IllegalArgumentException("namespace must not be null");
        }
        if (namespace.isBlank()) {
            throw new IllegalArgumentException("namespace must not be blank");
        }
    }

    private static boolean indexMatches(Index idx, String namespace, String indexToUse) {
        if (idx == null || indexToUse == null) {
            return false;
        }

        String indexName = idx.getName();
        if (indexName == null || !indexName.equals(indexToUse)) {
            return false;
        }

        return namespace.equals(idx.getNamespace());
    }
}
