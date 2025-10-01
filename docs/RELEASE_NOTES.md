# Release Notes

This document tracks the versions and changes for the `aerospike-expression-dsl` library.

## Version 0.1.0

**Release Date:** 2025-XX-XX

This is the initial developer preview release of the Aerospike Expression DSL.

### ✨ New Features

*   **DSL to Expression Translation**: Core functionality to parse a string-based DSL into native Aerospike Filter Expressions.
*   **Rich Operator Support**: Includes logical, comparison, and arithmetic operators.
*   **CDT Filtering**: Support for filtering on nested List and Map elements.
*   **Record Metadata Filtering**: Support for functions like `ttl()`, `sinceUpdate()`, and `deviceSize()`.
*   **Placeholder Support**: Parameterize expressions with `?` syntax for security and reusability.
*   **Automatic Secondary Index Optimization**: Ability to automatically convert parts of an expression into a `Filter` when an `IndexContext` is provided.

### ⚠️ Breaking Changes

*   As this is the first release, all APIs are new.

### 🐛 Bug Fixes

*   N/A

### 📚 Documentation

*   Initial creation of Getting Started guide, API reference, and usage examples.
