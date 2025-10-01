# Advanced Topic: Conditional Logic with `when`

The Expression DSL supports conditional logic through a `when` structure, similar to a `CASE` statement in SQL. This allows you to build sophisticated expressions that can return different values based on a set of conditions.

This is particularly useful for server-side data transformation or implementing complex business rules directly within a filter.

## `when` Syntax

The basic structure of a `when` expression is a series of `condition => result` pairs, optionally ending with a `default` clause.

```
when(
    condition1 => result1,
    condition2 => result2,
    ...,
    default => defaultResult
)
```

The server evaluates the conditions in order and returns the result for the *first* condition that evaluates to `true`. If no conditions are true, the `default` result is returned.

## Use Case: Tiered Logic

Imagine you want to categorize users into different tiers based on their `rank` bin, and then check if their `tier` bin matches that calculated category.

**Business Rules:**
*   If `rank` > 90, tier is "gold".
*   If `rank` > 70, tier is "silver".
*   If `rank` > 40, tier is "bronze".
*   Otherwise, the tier is "basic".

### DSL String

You can express this logic in a single DSL expression to verify a user's `tier`.

```
"$.tier == when($.rank > 90 => 'gold', $.rank > 70 => 'silver', $.rank > 40 => 'bronze', default => 'basic')"
```

Let's break this down:
1.  `when(...)` evaluates the inner logic first. If a record has `rank: 95`, the `when` block returns the string `'gold'`.
2.  The outer expression then becomes `$.tier == 'gold'`.
3.  The entire expression will return `true` if the `tier` bin for that record is indeed set to "gold", and `false` otherwise.

### Java Usage

```java
String dsl = "$.tier == when($.rank > 90 => 'gold', $.rank > 70 => 'silver', $.rank > 40 => 'bronze', default => 'basic')";

ExpressionContext context = ExpressionContext.of(dsl);
ParsedExpression parsed = parser.parseExpression(context, null);
Expression filter = Exp.build(parsed.getResult().getExp());

queryPolicy.filterExp = filter;
// This query will now return only the records where the tier bin is correctly set according to the rank.
```

## Using Placeholders with `when`

You can also use placeholders within a `when` expression for even greater flexibility.

### DSL String with Placeholders

```
"$.tier == when($.rank > ?0 => ?1, $.rank > ?2 => ?3, default => ?4)"
```

### Java Usage with Placeholders

```java
String dsl = "$.tier == when($.rank > ?0 => ?1, $.rank > ?2 => ?3, default => ?4)";

PlaceholderValues values = PlaceholderValues.of(
    90, "gold",      // rank > 90 => 'gold'
    70, "silver",    // rank > 70 => 'silver'
    "basic"          // default => 'basic'
);

ExpressionContext context = ExpressionContext.of(dsl, values);
// ...
```
**Note**: The example above has a different number of placeholders than values, please adjust accordingly. The correct `PlaceholderValues` would be:
```java
PlaceholderValues values = PlaceholderValues.of(
    90, "gold",
    70, "silver",
    40, "bronze",
    "basic"
);
```

The `when` structure enables you to push complex conditional logic directly to the server, reducing the need to pull data to the client for evaluation and minimizing data transfer.
