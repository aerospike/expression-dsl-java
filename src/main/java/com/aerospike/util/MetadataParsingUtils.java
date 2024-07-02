package com.aerospike.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aerospike.util.ParsingUtils.isInQuotes;

public class MetadataParsingUtils {

    /**
     * Checks if a given operand is a Metadata Expression.
     *
     * @param operand String operand
     * @return true if the operand is a Metadata Expression, false otherwise
     */
    public static boolean isMetadataExpression(String operand) {
        if (isInQuotes(operand)) {
            return false;
        }
        return operand.contains("(") && operand.contains(")");
    }

    /**
     * Extracts the Metadata Expression from the operand (removes unnecessary symbols and characters).
     *
     * @param operand String operand
     * @return Metadata Expression
     */
    public static String extractMetadataExpression(String operand) {
        int indexOfParenthesis = operand.indexOf("(");
        if (indexOfParenthesis != -1) {
            operand = operand.substring(0, indexOfParenthesis);
        }
        return operand.replace("$.", "");
    }

    /**
     * Checks if an operand contains a parameter, it is used for special metadata expressions
     * such as digestModulo(<int>) where an integer param is always provided.
     *
     * @param metadataExpression Metadata Expression Operand
     * @return true if contains a parameter, false otherwise
     */
    public static boolean metadataContainsParameter(String metadataExpression) {
        // Regular expression to match non-empty content within parentheses
        String regex = "\\([^)]{1,}\\)";

        // Compile the regex into a pattern
        Pattern pattern = Pattern.compile(regex);

        // Match the pattern against the input string
        Matcher matcher = pattern.matcher(metadataExpression);

        // Check if the pattern is found in the input string
        return matcher.find();
    }

    /**
     * Extracts parameter from a Metadata Expression, an example is digestModulo(3) that
     * always accepts an integer param, this method will extract the param value, in this case "3".
     *
     * @param metadataExpression Metadata Expression operand
     * @return param of the Metadata Expression
     */
    public static String extractParameterMetadataExpression(String metadataExpression) {
        // Regular expression to match non-empty content within parentheses
        String regex = "\\(([^)]+)\\)";

        // Compile the regex into a pattern
        Pattern pattern = Pattern.compile(regex);

        // Match the pattern against the input string
        Matcher matcher = pattern.matcher(metadataExpression);

        // If the pattern is found in the input string, return the content within the parentheses
        if (matcher.find()) {
            return matcher.group(1);
        }

        // If no match is found, return null
        return null;
    }
}
