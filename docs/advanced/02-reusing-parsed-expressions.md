# Advanced Topic: Reusing Parsed Expressions

For applications that execute the same filter logic repeatedly with different values, the Expression DSL offers a powerful performance optimization: reusing `ParsedExpression` objects.

## The Cost of Parsing

When you provide the `DSLParser` with a string, it performs several steps:
1.  **Lexing**: Breaks the string into a stream of tokens (e.g., `$.`, `age`, `>`, `100`).
2.  **Parsing**: Builds an Abstract Syntax Tree (AST) representing the logical structure of the expression.
3.  **Compilation**: Traverses the AST to create a template for the final Aerospike `Expression`.

This process has a small but non-zero CPU cost. If you are parsing the same string inside a tight loop (e.g., for every incoming web request), this cost can add up.

## The Solution: Parse Once, Substitute Many

The library is designed to separate the expensive parsing step from the cheap value-substitution step. The result of parsing is a `ParsedExpression` object, which is a thread-safe, reusable representation of your expression's structure.

You can then use this `ParsedExpression` object to generate a final `ParseResult` by providing new placeholder values. This substitution step is extremely fast as it bypasses the lexing and parsing entirely.

### High-Performance Workflow

1.  **On Application Startup**: Identify all the DSL queries your application will use. Parse each one *once* and store the resulting `ParsedExpression` objects in a cache (e.g., a `Map`).
2.  **During Request Handling**: When you need to run a query, retrieve the appropriate `ParsedExpression` from your cache, provide the current request's values via `PlaceholderValues`, and generate the final filter.

### Example: Caching Parsed Expressions

```java
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.api.DSLParser;
import com.aerospike.dsl.impl.DSLParserImpl;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueryService {

    private final DSLParser parser = new DSLParserImpl();
    private final Map<String, ParsedExpression> expressionCache = new ConcurrentHashMap<>();

    // --- Initialization (called once at startup) ---
    public void initialize() {
        // Parse all known queries and cache them
        String userQuery = "$.age > ?0 and $.city == ?1";
        expressionCache.put("FIND_USERS", parser.parseExpression(ExpressionContext.of(userQuery), null));

        String accountQuery = "$.balance < ?0";
        expressionCache.put("FIND_LOW_BALANCE_ACCOUNTS", parser.parseExpression(ExpressionContext.of(accountQuery), null));
        
        System.out.println("Expression cache initialized.");
    }

    // --- Business Logic (called many times) ---
    public RecordSet findUsersByAgeAndCity(int age, String city) {
        // 1. Retrieve the cached expression
        ParsedExpression parsedExpression = expressionCache.get("FIND_USERS");

        // 2. Provide the current values for the placeholders
        PlaceholderValues values = PlaceholderValues.of(age, city);

        // 3. Get the result (this is the fast part)
        ParseResult result = parsedExpression.getResult(values);

        // 4. Build and execute the query
        QueryPolicy policy = new QueryPolicy();
        policy.filterExp = Exp.build(result.getExp());
        
        Statement stmt = new Statement();
        stmt.setNamespace("test");
        stmt.setSetName("users");

        // return client.query(policy, stmt);
        System.out.printf("Executing query for age > %d and city == '%s'\n", age, city);
        return null; // Placeholder for actual query execution
    }
}
```

### When to Use this Pattern

*   When the same query logic is executed frequently.
*   In performance-critical code paths, such as low-latency APIs.
*   When your DSL strings are stored as constants.

By adopting this "parse once, reuse many" pattern, you can maximize the performance of the Expression DSL and build highly efficient, scalable applications.
