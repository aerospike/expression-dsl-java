package com.aerospike.dsl.model;

import com.aerospike.client.query.Filter;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SIndexFilter {

    @Getter
    protected final List<Filter> filters = new ArrayList<>();

    public SIndexFilter(Filter filter) {
        filters.add(filter);
    }

    public SIndexFilter(Collection<Filter> filters) {
        this.filters.addAll(filters);
    }
}
