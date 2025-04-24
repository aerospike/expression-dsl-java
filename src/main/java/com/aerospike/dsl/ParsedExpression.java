package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.exception.AerospikeDSLException;


/**
 * A class to store the results of DSL expression parsing: secondary index {@link Filter} or/and filter {@link Expression}.
 * <br>
 * Expression exists as long as DSL input String is valid.
 * <br>
 * List of Filters can be an empty if no suitable secondary index Filter was found.
 */
@Beta
@SuppressWarnings("LombokGetterMayBeUsed")
public class ParsedExpression {

    private final Exp filterExp;
    private Expression filterExpression;
    private final Filter siFilter;

    public ParsedExpression(Exp filterExp, Filter siFilter) {
        this.filterExp = filterExp;
        this.siFilter = siFilter;
    }

    /**
     * @return filter {@link Expression}. Can be null
     * @throws AerospikeDSLException If there was an error
     */
    public Expression getFilterExpression() {
        if (filterExp == null) {
            return null;
        }
        if (filterExpression == null) filterExpression = Exp.build(filterExp);
        return filterExpression;
    }

    /**
     * @return filter {@link Exp}. Can be null
     * @throws AerospikeDSLException If there was an error
     */
    public Exp getFilterExp() {
        return filterExp;
    }

    /**
     * @return secondary index {@link Filter}. Can be null
     */
    public Filter getSIFilter() {
        return siFilter;
    }
}
