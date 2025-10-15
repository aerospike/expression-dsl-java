# Conditional Logic: Control Structures `with` and  `when`

The Expression DSL supports conditional logic through `when` and `with` control structures, similar to a `CASE` statement in SQL. This allows you to build sophisticated expressions that can return different values based on a set of conditions.

This is particularly useful for server-side data transformation or implementing complex business rules.

## Control Structure `when`

The `when` structure enables you to push complex conditional logic directly to the server, reducing the need to pull data to the client for evaluation and minimizing data transfer.

The basic structure of a `when` expression is a series of `condition => result` pairs, optionally ending with a `default` clause:

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
*   If `rank` > 90, tier is "gold"
*   If `rank` > 70, tier is "silver"
*   If `rank` > 40, tier is "bronze"
*   Otherwise, the tier is "basic"

### DSL String

You can express this logic in a single DSL expression to verify a user's `tier`.

```
"$.tier == when($.rank > 90 => 'gold', $.rank > 70 => 'silver', $.rank > 40 => 'bronze', default => 'basic')"
```

Let's break this down:
1.  `when(...)` evaluates the inner logic first. If a record has `rank: 95`, the `when` block returns the string `'gold'`.
2.  The outer expression then becomes `$.tier == 'gold'`.
3.  The entire expression will return `true` if the `tier` bin for that record is indeed set to "gold", and `false` otherwise.

### Using Static DSL String in Java

```java
String dslString = "$.tier == when($.rank > 90 => 'gold', $.rank > 70 => 'silver', $.rank > 40 => 'bronze', default => 'basic')";

ExpressionContext context = ExpressionContext.of(dslString);
ParsedExpression parsed = parser.parseExpression(context);
Expression filter = Exp.build(parsed.getResult().getExp());

// Using Aerospike Java client
QueryPolicy queryPolicy = new QueryPolicy();

// Setting the resulting Expression as query filter
queryPolicy.filterExp = filter;
// This query will now return only the records where the tier bin is correctly set according to the rank.
```

### Using DSL String with Placeholders in Java

We can also use placeholders within a `when` expression for greater flexibility. Placeholders mark the places where values provided separately are matched by indexes.
This way the same DSL String can be used multiple times with different values for the same placeholders.

For example, let's add placeholders to our previous DSL expression and use the same API for generating an `Expression`:

```java
String dsl = "$.tier == when($.rank > ?0 => ?1, $.rank > ?2 => ?3, default => ?4)";

PlaceholderValues values = PlaceholderValues.of(
        90, "gold",
        70, "silver",
        40, "bronze",
        "basic"
);

ExpressionContext context = ExpressionContext.of(dsl, values);
ParsedExpression parsed = parser.parseExpression(context);
Expression filter = Exp.build(parsed.getResult().getExp());
// ...
```

The `when` structure enables you to push complex conditional logic directly to the server, reducing the need to pull data to the client for evaluation and minimizing data transfer.

## Control Structure `with`

The basic structure of a with expression allows you to declare temporary variables and use them within a subsequent expression:

```
with (
    var1 = val1,
    var2 = val2,
    ...
) do (expression)

```

The server evaluates the variable assignments in order, making each variable available for use in later assignments and in the final expression after the `do` keyword.

## Simple Example

For a simpler illustration of variable usage and calculations:

```
"with (x = 1, y = ${x} + 1) do (${x} + ${y})"
```

This expression:

1. Defines variable x with value 1
2. Defines variable y with value ${x} + 1 (which evaluates to 2)
3. Returns the result of ${x} + ${y} (which evaluates to 3)

The `with` structure enables us to create more readable and maintainable expressions by breaking complex logic into named variables. This is especially valuable when the same intermediate calculation is used multiple times in an expression or when the expression logic is complex.

## Use Case: More Complex Calculations

Imagine we want to calculate a user's eligibility score based on multiple factors like account `age`, `transaction history`, and `credit score`, then determine if they qualify for a premium service.

**Business Rules:**
*   Calculate a base score from account age
*   Add bonus points based on transaction count
*   Apply a multiplier based on credit score
*   User qualifies if final score exceeds threshold

### DSL String

We can use the `with` construct to make this complex calculation more readable and maintainable:

```
"with (
    baseScore = $.accountAgeMonths * 0.5,
    transactionBonus = $.transactionCount > 100 ? 25 : 0,
    creditMultiplier = $.creditScore > 700 ? 1.5 : 1.0,
    finalScore = (${baseScore} + ${transactionBonus}) * ${creditMultiplier}
) do (${finalScore} >= 75 && $.premiumEligible == true)"
```

Let's break this down:

1. We first calculate `baseScore` based on account age
2. We determine `transactionBonus` based on transaction count
3. We set `creditMultiplier` based on credit score
4. We calculate the `finalScore` using the previous variables
5. The final expression checks if the `finalScore` is at least 75 and if `premiumEligible` is true


### Using Static DSL String in Java

```java
String dslString = "with (" +
        "baseScore = $.accountAgeMonths * 0.5, " +
        "transactionBonus = $.transactionCount > 100 ? 25 : 0, " +
        "creditMultiplier = $.creditScore > 700 ? 1.5 : 1.0, " +
        "finalScore = (${baseScore} + ${transactionBonus}) * ${creditMultiplier}" +
        ") do (${finalScore} >= 75 && $.premiumEligible == true)";

ExpressionContext context = ExpressionContext.of(dslString);
ParsedExpression parsed = parser.parseExpression(context);
Expression filter = Exp.build(parsed.getResult().getExp());

// Using Aerospike Java client
QueryPolicy queryPolicy = new QueryPolicy();

// Setting the resulting Expression as query filter
queryPolicy.filterExp = filter;
// This query will return only records of users who qualify for premium service
```

### Using DSL String with Placeholders in Java

You can also use placeholders within a with expression for greater flexibility. Placeholders mark the places where values provided separately are matched by indexes.
This way the same DSL String can be used multiple times with different values for the same placeholders.

For example, let's add placeholders to our previous DSL expression and use the same API for generating an `Expression`:

```java
String dsl = "with (" +
        "baseScore = $.accountAgeMonths * ?0, " +
        "transactionThreshold = ?1, " +
        "transactionBonus = $.transactionCount > ${transactionThreshold} ? ?2 : 0, " +
        "creditThreshold = ?3, " +
        "creditMultiplier = $.creditScore > ${creditThreshold} ? ?4 : 1.0, " +
        "finalScore = (${baseScore} + ${transactionBonus}) * ${creditMultiplier}" +
        ") do (${finalScore} >= ?5 && $.premiumEligible == true)";

PlaceholderValues values = PlaceholderValues.of(
        0.5,    // Age multiplier
        100,    // Transaction threshold
        25,     // Transaction bonus
        700,    // Credit score threshold
        1.5,    // Credit multiplier
        75      // Minimum score threshold
);

ExpressionContext context = ExpressionContext.of(dsl, values);
ParsedExpression parsed = parser.parseExpression(context);
Expression filter = Exp.build(parsed.getResult().getExp());
// ...
```