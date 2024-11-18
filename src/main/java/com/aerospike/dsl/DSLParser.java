package com.aerospike.dsl;

import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;

import java.util.List;

public interface DSLParser {

    /**
     * Parse String DSL path into a filter Expression.
     * @param input
     * @return
     */
    Expression parseExpression(String input);

    /**
     * Parse String DSL path into a secondary index Filter.
     * @param input
     * @return
     */
    List<Filter> parseFilters(String input);
}
