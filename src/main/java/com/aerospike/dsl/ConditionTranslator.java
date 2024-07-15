package com.aerospike.dsl;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
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
        return Exp.build(visitor.visit(tree).getExp());
    }
}
