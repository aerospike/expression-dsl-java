# Troubleshooting & FAQ

This guide provides solutions to common problems and answers frequently asked questions about the Aerospike Expression DSL.

## Common Errors and Solutions

### `DslParseException`

This is the most common exception thrown by the library, indicating a problem with the DSL string itself.

| Cause | Example | Solution |
| :--- | :--- | :--- |
| **Mismatched Parentheses** | `($.age > 30` | Ensure every opening parenthesis `(` has a corresponding closing parenthesis `)`. |
| **Invalid Operator** | `$.age ** 30` | Check the spelling of all operators. Valid operators include `and`, `or`, `not`, `==`, `!=`, `>`, `>=`, `<`, `<=`, `+`, `-`, `*`, `/`, `%`. |
| **Unquoted String Literal** | `$.city == New York` | Enclose all string literals in single quotes: `$.city == 'New York'`. |
| **Incomplete Expression** | `$.age >` | Ensure every operator has the correct number of operands. |

### `IllegalArgumentException: Missing value for placeholder`

This error occurs when your DSL string contains placeholders (`?0`, `?1`, etc.), but you did not provide a corresponding value in the `PlaceholderValues` object.

**Example DSL:** `$.age > ?0 and $.city == ?1`

**Incorrect Code:**
```java
// Only one value provided, but two are needed
ExpressionContext.of(dsl, PlaceholderValues.of(30)); 
```

**Solution:** Ensure you provide a value for every placeholder in the correct order.

```java
ExpressionContext.of(dsl, PlaceholderValues.of(30, "New York"));
```

### Query is Slow or Scans the Entire Dataset

If your filtered query is not performing as expected, it may be because it is not leveraging a secondary index.

*   **Symptom**: A query on a highly selective field (e.g., a unique user ID) takes a long time.
*   **Cause**: The query is using a filter expression to scan all records on the server instead of using a secondary index to jump directly to the candidate records.
*   **Solution**:
    1.  Ensure a secondary index exists on the filter predicate.
    2.  Provide an `IndexContext` to the `parseExpression` method. This enables the parser to perform automatic query optimization. See the **"Leveraging Secondary Indexes Automatically"** guide for a detailed walkthrough.

## Frequently Asked Questions (FAQ)

**Q: What is the performance overhead of parsing a DSL string?**

A: The initial parsing of a DSL string into a `ParsedExpression` object has a small, one-time cost. However, the library is designed for high performance. Once parsed, the `ParsedExpression` can be cached and reused with different placeholder values. The `getResult(placeholderValues)` method is highly efficient as it simply substitutes values into the existing compiled structure without re-parsing. For best performance, parse your DSL expressions once and reuse the `ParsedExpression` object.

**Q: Is the `DSLParser` instance thread-safe?**

A: Yes. The default `DSLParserImpl` and the `ParsedExpression` objects it produces are thread-safe and can be safely shared across multiple threads.

**Q: Can I use the Expression DSL with the Aerospike REST Gateway?**

A: The Expression DSL is a Java library intended for use with the Aerospike Java Client. While the DSL *string* itself is portable, the REST Gateway has its own methods for filtering and does not directly consume this Java library. However, a portable expression language is a key feature, and future Aerospike tooling may adopt this standard.

**Q: Does the DSL support User-Defined Functions (UDFs)?**

A: The current version of the Expression DSL is focused on generating Aerospike Expressions for filtering and does not have syntax for invoking UDFs.
