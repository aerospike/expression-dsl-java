package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.AbstractPart;
import lombok.experimental.UtilityClass;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

@UtilityClass
public class ConditionTranslator {

    public static Expression translate(String input) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        ConditionParser parser = new ConditionParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.parse();

        ExpressionConditionVisitor visitor = new ExpressionConditionVisitor();
        AbstractPart abstractPart = visitor.visit(tree);

        // When we can't identify a specific case of syntax error, we throw a generic DSL syntax error instead of NPE
        if (abstractPart == null) {
            throw new AerospikeDSLException("Could not parse given input, wrong syntax");
        }
        Exp expResult = abstractPart.getExp();
        return Exp.build(expResult);
    }
}
