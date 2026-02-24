# Guide: Automatic Secondary Index Optimization

One of the most powerful features of the Expression DSL is its ability to automatically leverage accessible Aerospike secondary indexes (SI).

When you provide the parser with a list of available indexes, it can analyze your DSL string and transform a part of it into a more performant secondary index filter.

## Why is this important?

*   **Performance**: A secondary index query is significantly faster than a full scan with a filter expression. An SI query allows the database to jump directly to the records that might match, whereas a filter expression requires the server to read every single record in the set and evaluate the expression against it.
*   **Simplicity**: You don't need to manually decide which parts of your query should use an index. You can write a single, logical DSL string, and the parser will perform the optimization for you given that you 

## How it Works

When you call `parser.parseExpression()`, you can optionally provide an `IndexContext`. This object tells the parser which namespace you are querying and which secondary indexes are available.

The parser then does the following:
1.  It analyzes the `and` components of your DSL expression if there are any.
2.  It compares each component against the list of available indexes.
3.  If it finds a component that can be satisfied by a numeric or string range query on an indexed bin, it converts that component into an Aerospike secondary index `Filter` object.
4.  It marks that component in order not to use it when building `Expression`.
5.  The rest of the DSL string is converted into an `Expression`.
6.  The `ParseResult` then contains either one of the following entities or **both**:
* `Filter` (for the SI query) - given that the correct `IndexContext` was provided, and that the given DSL expression can be converted to a secondary index filter
* `Expression` (for the scan expression filter)

> **Note:** Only one secondary index can be used per query. The index will be chosen based on cardinality (preferring indexes with a higher `binValuesRatio`), otherwise alphabetically.

## Usage Example

Let's assume you have a secondary index named `idx_users_city` on the `city` bin in the `users` set.

### 1. Define the Index Information

First, you need to represent your available index in code.

```java
import com.aerospike.client.query.IndexType;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
import java.util.List;

// Describe the available secondary index (namespace, bin, indexType, binValuesRatio are required)
Index cityIndex = Index.builder()
    .namespace("test")
    .bin("city")
    .indexType(IndexType.STRING)
    .binValuesRatio(1) // Cardinality from Aerospike sindex-stat or set manually
    .name("idx_users_city")
    .build();

// Create index context (namespace must not be null or blank)
IndexContext indexContext = IndexContext.of("test", List.of(cityIndex));
```

### 2. Parse the Expression using IndexContext

Now, provide the `indexContext` when you parse your DSL string.

**DSL String:**
```
"$.city == 'New York' and $.age > 30"
```

**Java Code:**
```java
DSLParser parser = new DSLParserImpl();
String dsl = "$.city == 'New York' and $.age > 30";
ExpressionContext context = ExpressionContext.of(dsl);

// Provide the IndexContext to enable using SI filter
ParsedExpression parsed = parser.parseExpression(context, indexContext);
ParseResult result = parsed.getResult();
```

### 3. Extract Both Filter and Expression
```java

// 3. Extract Both Filter and Expression
Filter siFilter = result.getFilter(); // This will be non-null if indexes are correct and DSL string input allows building SI filter, like in this example
Expression filterExpression = Exp.build(result.getExp()); // This will contain the scan expression filter

// The parser has split the query:
// siFilter is now a Filter.equal("city", "New York")
// filterExpression is now Exp.gt(Exp.intBin("age"), Exp.val(30))
```
### 4. Execute the Query

When you execute the query, you need to use both the `Filter` and the `Expression`. The Java client handles this by applying the secondary index `Filter` first to select the initial set of records, and then applying the `filterExp` on the server to those results.

```java
Statement stmt = new Statement();
stmt.setNamespace("test");
stmt.setSetName("users");

// Apply the secondary index filter
stmt.setFilter(siFilter);

// Apply the remaining filter expression
QueryPolicy queryPolicy = new QueryPolicy();
queryPolicy.filterExp = filterExpression;

// Execute the highly optimized query
client.query(queryPolicy, stmt);
```

By providing the `IndexContext`, you have allowed the DSL parser to transform a potentially slow scan into a fast, targeted query, without changing your original DSL logic.