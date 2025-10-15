# Guide: Writing Your First Expression

This guide covers the fundamental syntax of the Aerospike Expression DSL. You will learn how to filter records based on bin values and combine multiple conditions.

## Anatomy of the DSL

The Expression DSL is a functional language for applying predicates to Aerospike bin data and record metadata. Here’s a breakdown of a simple example:

```
$.binName > 100
 ^    ^   ^  ^
 |    |   |  |
 |    |   |  +---- Value (can be integer, float, or 'string')
 |    |   +------- Comparison Operator (==, !=, >, <, etc.)
 |    +----------- Bin Name and / or Function
 +---------------- Path Operator (always starts with $.)
```

### Path Operator (`$.`)

All expressions start with `$.` to signify the root of the record. You follow it with the name of a bin (e.g., `$.name`) or a metadata function (e.g., `$.lastUpdate()`).

### Operators

The DSL supports a rich set of operators.

*   **Comparison**: `==`, `!=`, `>`, `>=`, `<`, `<=`
*   **Logical**: `and`, `or`, `not()`, `exclusive()`
*   **Arithmetic**: `+`, `-`, `*`, `/`, `%`

### Values

*   **Integers**: `100`, `-50`
*   **Floats**: `123.45`
*   **Strings**: Must be enclosed in single quotes, e.g., `'hello'`, `'US'`.
*   **Booleans**: `true`, `false`

## Filtering on Bin Values

Here are some examples of basic filters on different data types.

### Numeric Bins

To filter on a bin containing an integer or float, use standard comparison operators.

**DSL String:**
```
"$.age >= 30"
```

**Java Usage:**
```java
ExpressionContext context = ExpressionContext.of("$.age >= 30");
ParsedExpression parsed = parser.parseExpression(context);
QueryPolicy queryPolicy = new QueryPolicy();
queryPolicy.filterExp = Exp.build(parsed.getResult().getExp());
```

### String Bins

Remember to enclose string literals in single quotes.

**DSL String:**
```
"$.country == 'US'"
```

**Java Usage:**
```java
ExpressionContext context = ExpressionContext.of("$.country == 'US'");
// ...
```

### Boolean Bins

**DSL String:**
```
"$.active == true"
```

## Combining Conditions with Logical Operators

You can build complex filters by combining conditions with `and` and `or`. Use parentheses `()` to control the order of evaluation.

### `and` Operator

Returns records that match **all** conditions.

**DSL String:**
```
"$.age > 30 and $.country == 'US'"
```

### `or` Operator

Returns records that match **at least one** of the conditions.

**DSL String:**
```
"$.tier == 'premium' or $.logins > 100"
```

### `not()` Operator

Negates a condition.

**DSL String:**
```
"not($.country == 'US')"
```

### `exclusive()` Operator

Creates an expression that returns true if only one of its parts is true.

**DSL String:**
```
"exclusive($.x < '5', $.x > '5')"
```

### Controlling Precedence with Parentheses

Just like in mathematics, you can use parentheses to group expressions and define the order of operations. The `and` operator has a higher precedence than `or`.

Consider this expression:
```
"$.age > 65 or $.age < 18 and $.isStudent == true"
```

This is evaluated as `$.age > 65 or ($.age < 18 and $.isStudent == true)`.

To get the intended logic, use parentheses:
```
"($.age > 65 or $.age < 18) and $.isStudent == true"
```
This expression correctly filters for users who are either over 65 or under 18, and who are also students.