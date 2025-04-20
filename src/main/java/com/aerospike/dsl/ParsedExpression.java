package com.aerospike.dsl;

import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.exception.AerospikeDSLException;
import lombok.AllArgsConstructor;


/**
 * A class to store the results of DSL expression parsing: secondary index {@link Filter} or/and filter {@link Expression}.
 * <br>
 * Expression exists as long as DSL input String is valid.
 * <br>
 * List of Filters can be an empty if no suitable secondary index Filter was found.
 */
@AllArgsConstructor
@Beta
@SuppressWarnings("LombokGetterMayBeUsed")
public class ParsedExpression {

    Expression filterExpression;
    Filter siFilter;

    /**
     * @return filter {@link Expression}. Can be null
     * @throws AerospikeDSLException If there was an error
     */
    public Expression getFilterExpression() {
        return filterExpression;
    }

    /**
     * @return secondary index {@link Filter}. Can be null
     */
    public Filter getSIFilter() {
        return siFilter;
    }
}
