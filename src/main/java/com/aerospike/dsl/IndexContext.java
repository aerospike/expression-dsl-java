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
    /**
     * The original (un-narrowed) index collection, or {@code null} when no hint-based narrowing occurred.
     * Used to fall back to automatic selection when the narrowed set cannot produce a filter.
     */
    private final Collection<Index> fallbackIndexes;

    private IndexContext(String namespace, Collection<Index> indexes) {
        this(namespace, indexes, null);
    }

    private IndexContext(String namespace, Collection<Index> indexes, Collection<Index> fallbackIndexes) {
        this.namespace = namespace;
        this.indexes = indexes;
        this.fallbackIndexes = fallbackIndexes;
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
     * @param indexToUse The name of an index to use. If null, blank, or not found in the collection,
     *                   index is chosen automatically by cardinality then alphabetically.
     *                   If found, that index is tried first; if it cannot be applied to the
     *                   expression (e.g. type mismatch), selection falls back to all indexes
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext of(String namespace, Collection<Index> indexes, String indexToUse) {
        validateNamespace(namespace);
        if (indexes == null || indexToUse == null || indexToUse.isBlank()) {
            return new IndexContext(namespace, indexes);
        }
        List<Index> matchingIndexes = indexes.stream()
                .filter(idx -> indexMatches(idx, namespace, indexToUse))
                .toList();
        if (matchingIndexes.isEmpty()) {
            return new IndexContext(namespace, indexes);
        }
        return new IndexContext(namespace, matchingIndexes, indexes);
    }

    /**
     * Create index context with a bin name hint specifying which bin's index to use.
     * If one or more indexes in the collection match the given bin name and namespace, only those
     * indexes are used for selection. When exactly one matches, it is used directly. When multiple
     * match (e.g., a STRING index and a NUMERIC index on the same bin), automatic type-based and
     * cardinality-based selection is applied within that narrowed set, allowing the correct index
     * type to be inferred from the query (e.g., a numeric comparison picks the NUMERIC index).
     * If there is no match, the hint is ignored and the parser falls back to fully automatic
     * selection across all indexes.
     *
     * @param namespace Namespace to be used for creating {@link com.aerospike.dsl.client.query.Filter}.
     *                  Must not be null or blank
     * @param indexes   Collection of {@link Index} objects to be used for creating Filter
     * @param binToUse  The name of the bin whose index should be used. If not found, null, or blank,
     *                  index is chosen automatically across all indexes
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext withBinHint(String namespace, Collection<Index> indexes, String binToUse) {
        validateNamespace(namespace);
        if (indexes == null || binToUse == null || binToUse.isBlank()) {
            return new IndexContext(namespace, indexes);
        }
        List<Index> matchingIndexes = indexes.stream()
                .filter(idx -> binMatches(idx, namespace, binToUse))
                .toList();
        if (matchingIndexes.isEmpty()) {
            return new IndexContext(namespace, indexes);
        }
        return new IndexContext(namespace, matchingIndexes, indexes);
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
