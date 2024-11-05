# Aerospike Expression DSL
[![Build project](https://github.com/citrusleaf/expression-dsl-java/actions/workflows/build.yml/badge.svg)](https://github.com/citrusleaf/expression-dsl-java/actions/workflows/build.yml)

> [!NOTE]
> This is a developer preview, not yet ready for production use, but with future releases planned. We appreciate feedback from the Aerospike 
> developer community through GitHub issues or the [Aerospike Developers Discord](https://aerospike.com/community/).

Aerospike Expression DSL is a Java library that allows translating a canonical Aerospike Expression DSL string into an Aerospike Expression.

[Aerospike Expressions](https://aerospike.com/docs/server/guide/expressions) is a functional language for applying predicates to bin data and record metadata.  
Expressions are essential to different areas of server functionality:
* Secondary indexes
* Filter Expressions (introduced in Aerospike Database 5.2)
* XDR Filter Expressions (introduced in Aerospike Database 5.3)
* Operation Expressions (introduced in Aerospike Database 5.6)

## Why DSL
A filter expression is created by combining the expression predicate classes using Polish notation, compiling it, and then attaching it to the operation.
This approach has several limitations, including a difficult developer experience.

Expressions require the use of native language clients. As a result, filter expressions cannot be used in places that only accept a filter written in text.
As a workaround, expressions can be created separately using a native client and exported as a base64-encoded string.

A text-based DSL can be converted into Expressions and be used across frameworks, connectors, gateways, data browsers, IDE plug-ins, and all language-native clients.
Having a standard way to describe filters is easy to document and makes it accessible to developers regardless of their language preferences.

## Prerequisites
* Java 17 or later
* Aerospike Server version 5.2+

## Build from source
Aerospike Expression DSL uses [ANTLR v4](https://github.com/antlr/antlr4) for its parsing capabilities.  
When modifying the grammar file you will need to re-generate the ANTLR sources by running the following command:

`mvn clean generate-sources compile`

## Usage examples

### Filter expression
```java
String input = "$.intBin1 > 100";
Expression expression = ConditionTranslator.translate(input);
```
Will return the following expression:
```java
Exp.build(
    Exp.gt(
        Exp.intBin("intBin1"),
        Exp.val(100))
);
```

## License
Licensed under the Apache 2.0 License.
