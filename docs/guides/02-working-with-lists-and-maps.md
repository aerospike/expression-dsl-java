# Guide: Working with Lists and Maps

The Expression DSL provides a powerful and intuitive syntax for filtering on Complex Data Types (CDTs), such as Lists and Maps. This guide will show you how to query inside these nested structures.

## Accessing Map Values

You can access values within a map bin using standard dot notation for string keys.

### Filtering by Map Value

Let's say you have a `user` bin that is a map containing profile information.

**Record Data:**
```json
{
  "user": {
    "name": "Alice",
    "email": "alice@example.com",
    "logins": 150
  }
}
```

**DSL String:**
To find users with more than 100 logins, you can write:
```
"$.user.logins > 100"
```

**Java Usage:**
```java
ExpressionContext context = ExpressionContext.of("$.user.logins > 100");
// ...
```

### Accessing with Non-Standard Keys

If a map key is not a valid identifier (e.g., it contains spaces or special characters), you can use bracket notation with single quotes.

**Record Data:**
```json
{
  "metrics": {
    "daily-logins": 25
  }
}
```

**DSL String:**
```
"$.metrics['daily-logins'] > 20"
```

## Accessing List Elements

You can access elements within a list bin using bracket notation `[]`.

### Filtering by List Index

Indexes are 0-based. To access the first element, use `[0]`.

**Record Data:**
Let's say you have a `scores` bin containing a list of test scores.
```json
{
  "scores": [88, 95, 72]
}
```

**DSL String:**
To find records where the first score is greater than 90:
```
"$.scores.[0] > 90"
```

## Querying Nested Structures

The real power of the DSL shines when you combine these accessors to query deeply nested data.

### Map containing a List

Imagine a `user` bin where one of the map values is a list of roles.

**Record Data:**
```json
{
  "user": {
    "name": "Bob",
    "roles": ["admin", "editor"]
  }
}
```

**DSL String:**
To find a user whose first role is "admin":
```
"$.user.roles.[0] == 'admin'"
```

### List containing a Map

Now consider a list of `devices`, where each element is a map.

**Record Data:**
```json
{
  "devices": [
    { "type": "phone", "os": "iOS" },
    { "type": "laptop", "os": "linux" }
  ]
}
```

**DSL String:**
To find records where the second device is a laptop:
```
"$.devices.[1].type == 'laptop'"
```

## Advanced CDT Queries

The DSL also supports more advanced CDT selectors.

### List Rank and Value

*   `[#<rank>]`: Selects an element by its rank (sorted order). `[#-1]` selects the largest element.
*   `[=<value>]`: Selects elements by their value.

**Record Data:**
```json
{
  "numbers": [10, 50, 20, 40, 30]
}
```

**DSL String (Rank):**
To check if the largest number in the list is 50:
```
"$.numbers.[#-1] == 50"
```

**DSL String (Value):**
The DSL can be used to check for the existence of a value or compare it. For example, to find if `20` exists and is greater than `10`:
```
"$.numbers.[=20] > 10"
```

### Functions on CDTs

You can call functions on CDT bins, such as getting the size.

**DSL String:**
To find records where the `devices` list contains more than 1 item:
```
"$.devices.count() > 1"
```
You can also see this written as `$.devices.[].count() > 1`, which is equivalent.

This powerful syntax allows you to push complex data filtering directly to the Aerospike server, leading to highly efficient queries.
