package com.aerospike.dsl.util;

import lombok.NonNull;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ParsingUtils {

    /**
     * Get the string inside the quotes.
     *
     * @param str String input
     * @return String inside the quotes
     */
    public static String getWithoutQuotes(String str) {
        if (str.length() > 2) {
            return str.substring(1, str.length() - 1);
        } else {
            throw new IllegalArgumentException("String %s must contain more than 2 characters".formatted(str));
        }
    }

    /**
     * @param a Integer, can be null
     * @param b Integer, non-null
     * @return a - b if a != null, otherwise null
     */
    public static Integer subtractNullable(Integer a, @NonNull Integer b) {
        return a == null ? null : a - b;
    }
}
