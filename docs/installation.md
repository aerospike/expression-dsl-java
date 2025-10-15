# Installation & Setup

This guide provides detailed instructions for adding the Aerospike Expression DSL library to your project and configuring it.

## Prerequisites

*   **Java 17 or later**
*   **Aerospike Server 5.7 or later**
*   A build tool such as **Maven** or **Gradle**.

## Library Installation

To use the Expression DSL, you need to add two dependencies to your project: `aerospike-expression-dsl` and the core `aerospike-client-jdk8`.

### Maven

Add the following dependencies to your `pom.xml` file:

```xml
<dependencies>
    <!-- The Expression DSL Library -->
    <dependency>
        <groupId>com.aerospike</groupId>
        <artifactId>aerospike-expression-dsl</artifactId>
        <version>0.1.0</version>
    </dependency>

    <!-- The core Aerospike Java Client -->
    <!-- Ensure this version is compatible with your server version -->
    <dependency>
        <groupId>com.aerospike</groupId>
        <artifactId>aerospike-client-jdk8</artifactId>
        <version>8.1.1</version>
    </dependency>
</dependencies>
```

### Gradle

Add the following to your `build.gradle` file's `dependencies` block:

```groovy
dependencies {
    // The Expression DSL Library
    implementation 'com.aerospike:aerospike-expression-dsl:0.1.0'

    // The core Aerospike Java Client
    // Ensure this version is compatible with your server version
    implementation 'com.aerospike:aerospike-client-jdk8:8.1.1'
}
```

## Initializing the Parser

The main entry point for the library is the `DSLParser` interface. To get started, simply instantiate the default implementation:

```java
import com.aerospike.dsl.api.DSLParser;
import com.aerospike.dsl.impl.DSLParserImpl;

// Create a reusable parser instance
DSLParser parser = new DSLParserImpl();
```

This `parser` object is thread-safe and can be reused across your application to parse different DSL expression strings.

## Compatibility Matrix

It is important to ensure the versions of the DSL library, Java client, and Aerospike Server are compatible.

| `aerospike-expression-dsl` | `aerospike-client-jdk8` | Aerospike Server |
| :--- | :--- |:-----------------|
| 0.1.0 | 8.0.0+ | 5.7+             |

## Building from Source (Optional)

If you need to build the library from source, you will need to regenerate the ANTLR sources first. 

kThe grammar file is located at `src/main/antlr4/com/aerospike/dsl/Condition.g4`.

Run the following Maven command to re-generate the Java parser classes:

```sh
mvn clean generate-sources compile
```