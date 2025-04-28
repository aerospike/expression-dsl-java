package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.AbstractPart;
import lombok.Getter;

import java.util.Map;


/**
 * A class to build and store the results of DSL expression parsing: secondary index {@link Filter}
 * and/or filter {@link Exp} and {@link Expression}.
 */
@Beta
@Getter
public class ParsedExpression {

    private final AbstractPart resultingPart;
    private final String namespace;
    private final Map<String, Index> indexesMap;
    private Pair<Filter, Exp> resultPair;

    public ParsedExpression(AbstractPart resultingPart, String namespace, Map<String, Index> indexesMap) {
        this.resultingPart = resultingPart;
        this.namespace = namespace;
        this.indexesMap = indexesMap;
    }

    /**
     * @return Pair of secondary index {@link Filter} and filter {@link Exp}. Each can be null in case of an invalid or
     * unsupported DSL string
     * @throws AerospikeDSLException If there was an error
     */
    public Pair<Filter, Exp> getResultPair() {
        if (resultPair == null) {
            resultPair = DSLParserImpl.getResultPair(resultingPart, namespace, indexesMap);
        }
        return resultPair;
    }
}
