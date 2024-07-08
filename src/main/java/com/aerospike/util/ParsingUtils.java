package com.aerospike.util;

public class ParsingUtils {

    /**
     * Checks if the given string is inside quotes.
     *
     * @param str String input
     * @return true if is in quotes, otherwise false
     */
    public static boolean isInQuotes(String str) {
        return (str.startsWith("\"") && str.endsWith("\"")) || (str.startsWith("'") && str.endsWith("'"));
    }

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
            throw new IllegalArgumentException(String.format("String %s must contain more than 2 characters", str));
        }
    }
}
