package com.aerospike.dsl.model;

import com.aerospike.client.query.Filter;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class SIndexFilter {

    protected final List<Filter> filters = new ArrayList<>();

    public SIndexFilter(Filter filter) {
        addFilter(filter);
    }

    public SIndexFilter(List<Filter> filters) {
        this.filters.addAll(filters);
    }

    public void addFilter(Filter filter) {
        if (filter != null) filters.add(filter);
    }
}
