# Aerospike Expression DSL

Aerospike Expression DSL aims to simplify the way developers interact with Filter Expressions by translating
a well-structured, simplified Aerospike Expression DSL Text into Aerospike Expressions.

Aerospike Expression DSL uses [ANTLR v4](https://github.com/antlr/antlr4) under the hood.

## Example
```
String input = "$.intBin1 > 100";
Expression expression = ConditionTranslator.translate(input);
```
Will return the following expression:
```
Exp.build(
    Exp.gt(
        Exp.intBin("intBin1"),
        Exp.val(100))
);
```

## Generate ANTLR Sources
When modifying the grammar file you will need to re-generate the sources of ANTLR by running the following command:

`mvn clean generate-sources compile`