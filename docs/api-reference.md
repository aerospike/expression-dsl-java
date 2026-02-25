# API Reference

The Expression DSL library has a concise API. This document highlights the most important classes and methods you will interact with.

For a complete, detailed reference, please consult the Javadoc for the library.

## Core Classes

### `com.aerospike.dsl.api.DSLParser`

This is the main interface and entry point for the library.

*   **`ParsedExpression parseExpression(ExpressionContext input)`**
    *   **Description**: The primary method used to parse a DSL string when no secondary indexes are provided. It returns a `ParsedExpression` object that contains the compiled result, which can be reused.
*   **`ParsedExpression parseExpression(ExpressionContext input, IndexContext indexContext)`**
    *   **Description**: The primary method used to parse a DSL string. It returns a `ParsedExpression` object that contains the compiled result, which can be reused.
    *   **Parameters**:
        *   `input`: An `ExpressionContext` object containing the DSL string and any placeholder values.
    *   **Description**: The primary method used to parse a DSL string. It returns a `ParsedExpression` object that contains the compiled result, which can be reused.
    *   **Parameters**:
        *   `input`: An `ExpressionContext` object containing the DSL string and any placeholder values.
        *   `indexContext`: An optional `IndexContext` object containing a list of available secondary indexes for query optimization. Can be `null`.
    *   **Returns**: A `ParsedExpression` object representing the compiled expression tree.

### `com.aerospike.dsl.ExpressionContext`

This class is a container for the DSL string and any values to be substituted for placeholders.

*   **`static ExpressionContext of(String dslString)`**: Creates a context for a DSL string without placeholders.
*   **`static ExpressionContext of(String dslString, PlaceholderValues values)`**: Creates a context for a DSL string that uses `?` placeholders, providing the values to be substituted.

### `com.aerospike.dsl.ParsedExpression`

This object represents the compiled, reusable result of a parsing operation. It is thread-safe.

*   **`ParseResult getResult()`**: Returns the final `ParseResult` for an expression that does not contain placeholders.
*   **`ParseResult getResult(PlaceholderValues values)`**: Returns the final `ParseResult` by substituting the given placeholder values into the compiled expression tree. This is highly efficient as it bypasses the parsing step.

### `com.aerospike.dsl.ParseResult`

This class holds the final, concrete outputs of the parsing and substitution process.

*   **`Filter getFilter()`**: Returns an Aerospike `Filter` object if the parser was able to optimize a portion of the DSL string into a secondary index query. Returns `null` if no optimization was possible.
*   **`com.aerospike.client.exp.Expression.Exp getExp()`**: Returns the Aerospike `Exp` object representing the DSL filter logic. This is the part of the expression that will be executed on the server for records that pass the secondary index filter. If the entire DSL string was converted into a `Filter`, this may be `null`.

### `com.aerospike.dsl.IndexContext`

A container for the information required for automatic secondary index optimization.

*   **`static IndexContext of(String namespace, Collection<Index> indexes)`**: Creates a context.
    *   `namespace`: The namespace the query will be run against. Must not be null or blank.
    *   `indexes`: A collection of `Index` objects representing the available secondary indexes for that namespace.
*   **`static IndexContext of(String namespace, Collection<Index> indexes, String indexToUse)`**:
    Creates a context with an explicit index name hint.
    *   `namespace`: The namespace the query will be run against. Must not be null or blank.
    *   `indexes`: A collection of `Index` objects representing the available secondary indexes for that namespace.
    *   `indexToUse`: The name of the index to use for the secondary index filter.
    If not found, `null`, or empty, the index is chosen automatically by cardinality
    (higher `binValuesRatio` preferred), then alphabetically by bin name.
*   **`static IndexContext withBinHint(String namespace, Collection<Index> indexes, String binToUse)`**:
    Creates a context with an explicit bin name hint. Use this when you want to direct the parser to
    an index on a specific bin without knowing the index name.
    *   `namespace`: The namespace the query will be run against. Must not be null or blank.
    *   `indexes`: A collection of `Index` objects representing the available secondary indexes for that namespace.
    *   `binToUse`: The name of the bin whose index should be used. If exactly one index in the collection
    matches the given bin name and namespace, that index is used. If the bin matches multiple indexes,
    no index matches, the value is `null`, or the value is blank, the parser falls back to fully automatic
    selection (cardinality, then alphabetically).

### `com.aerospike.dsl.Index`

Represents an available secondary index for optimization.

*   **Mandatory fields**: `namespace`, `bin`, `indexType`, `binValuesRatio`.
*   **`binValuesRatio` validation**:
    * Must be explicitly provided.
    * Must be non-negative (`>= 0`).

## Example API Flow

Here is a recap of how the classes work together in a typical use case:

```java
// 1. Get a parser instance
DSLParser parser = new DSLParserImpl();

// 2. Define the context for the expression and placeholders
String dsl = "$.age > ?0";
ExpressionContext context = ExpressionContext.of(dsl, PlaceholderValues.of(30));

// (Optional) Define the index context for optimization
IndexContext indexContext = IndexContext.of("namespace", availableIndexes);

// 3. Parse the expression once to get a reusable object
ParsedExpression parsedExpression = parser.parseExpression(context, indexContext);

// 4. Get the final result by substituting values
// This step can be repeated many times with different values
ParseResult result = parsedExpression.getResult(); 

// 5. Extract the Filter and Expression for use in a QueryPolicy
Filter siFilter = result.getFilter();
Expression filterExp = Exp.build(result.getExp());

QueryPolicy policy = new QueryPolicy();
policy.filterExp = filterExp;
// Note: The Java client does not have a separate field for the secondary index filter.
// The filter is applied by the client before sending the query.
```