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
     * @param indexToUse The name of an index to use. If not found, null, or empty, index is chosen
     *                   by cardinality or alphabetically
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext of(String namespace, Collection<Index> indexes, String indexToUse) {
        validateNamespace(namespace);
        List<Index> matchingIndexes = indexes.stream()
                .filter(idx -> indexMatches(idx, namespace, indexToUse))
                .toList();
        return new IndexContext(namespace, matchingIndexes.isEmpty() ? indexes : matchingIndexes);
    }

    /**
     * Create index context with a bin name hint specifying which bin's index to use.
     * If exactly one index in the collection matches the given bin name and namespace,
     * that index is used. Otherwise, all indexes are kept and selection falls back to
     * the automatic cardinality / alphabetical strategy.
     *
     * @param namespace Namespace to be used for creating {@link com.aerospike.dsl.client.query.Filter}.
     *                  Must not be null or blank
     * @param indexes   Collection of {@link Index} objects to be used for creating Filter
     * @param binToUse  The name of the bin whose index should be used. If not found, null,
     *                  blank, or matches multiple indexes, index is chosen automatically
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext withBinHint(String namespace, Collection<Index> indexes, String binToUse) {
        validateNamespace(namespace);
        if (binToUse == null || binToUse.isBlank()) {
            return new IndexContext(namespace, indexes);
        }
        List<Index> matchingIndexes = indexes.stream()
                .filter(idx -> binMatches(idx, namespace, binToUse))
                .toList();
        return new IndexContext(namespace, matchingIndexes.size() == 1 ? matchingIndexes : indexes);
    }

    private static void validateNamespace(String namespace) {
        if (namespace == null) {
            throw new IllegalArgumentException("namespace must not be null");
        }
        if (namespace.isBlank()) {
            throw new IllegalArgumentException("namespace must not be blank");
        }
    }

    private static boolean binMatches(Index idx, String namespace, String binToUse) {
        if (idx == null || binToUse == null) {
            return false;
        }

        String binName = idx.getBin();
        if (binName == null || !binName.equals(binToUse)) {
            return false;
        }

        return namespace.equals(idx.getNamespace());
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
