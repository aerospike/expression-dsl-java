package com.aerospike.dsl;

import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;

import java.util.List;

public interface DSLParser {

    Expression parseExpression(String input);

    List<Filter> parseFilters(String input);
}
