package com.aerospike.dsl.util;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.math.BigInteger;

@UtilityClass
public class ParsingUtils {

    private static final BigInteger LONG_MIN_ABS = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    private static final BigInteger INT_MIN_VALUE = BigInteger.valueOf(Integer.MIN_VALUE);
    private static final BigInteger INT_MAX_VALUE = BigInteger.valueOf(Integer.MAX_VALUE);

    /**
     * Extracts a signed integer value from a {@code signedInt} parser rule context.
     * The grammar rule is {@code signedInt: '-'? INT;}, so the context contains
     * either just an INT token or a '-' followed by an INT token.
     *
     * @param ctx The signedInt context from the parser
     * @return The parsed integer value, negated if a '-' prefix is present
     */
    public static int parseSignedInt(ConditionParser.SignedIntContext ctx) {
        String intText = ctx.INT().getText();
        boolean isNegative = ctx.getText().startsWith("-");
        if (isHexOrBinaryIntToken(intText)) {
            throw new DslParseException("Only decimal integer literals are supported in this element: " + ctx.getText());
        }

        BigInteger signedValue = getBigInteger(ctx, intText, isNegative);

        return signedValue.intValue();
    }

    private static BigInteger getBigInteger(ConditionParser.SignedIntContext ctx, String intText, boolean isNegative) {
        BigInteger value;
        try {
            value = new BigInteger(intText, 10);
        } catch (NumberFormatException e) {
            throw new DslParseException("Invalid integer literal: " + ctx.getText(), e);
        }
        BigInteger signedValue = isNegative ? value.negate() : value;

        if (signedValue.compareTo(INT_MIN_VALUE) < 0 || signedValue.compareTo(INT_MAX_VALUE) > 0) {
            throw new DslParseException("Signed integer literal out of range for INT: " + ctx.getText());
        }
        return signedValue;
    }

    private static boolean isHexOrBinaryIntToken(String intText) {
        return intText.length() > 2
                && intText.charAt(0) == '0'
                && (intText.charAt(1) == 'x' || intText.charAt(1) == 'X'
                || intText.charAt(1) == 'b' || intText.charAt(1) == 'B');
    }

    /**
     * Parses an unsigned INT token (decimal, hex or binary) into a long value.
     * The value range is [0, 2^63], where 2^63 is represented as {@link Long#MIN_VALUE}.
     *
     * @param text INT token text
     * @return Parsed long value (unsigned 2^63 maps to {@link Long#MIN_VALUE})
     */
    public static long parseUnsignedLongLiteral(String text) {
        BigInteger value = parseUnsignedIntegerLiteral(text);
        if (value.compareTo(LONG_MIN_ABS) > 0) {
            throw new DslParseException("Integer literal out of range: " + text);
        }
        return value.longValue();
    }

    private static BigInteger parseUnsignedIntegerLiteral(String text) {
        try {
            int radix = 10;
            String digits = text;
            if (text.length() > 2 && text.charAt(0) == '0') {
                char prefix = text.charAt(1);
                if (prefix == 'x' || prefix == 'X') {
                    radix = 16;
                    digits = text.substring(2);
                } else if (prefix == 'b' || prefix == 'B') {
                    radix = 2;
                    digits = text.substring(2);
                }
            }
            return new BigInteger(digits, radix);
        } catch (NumberFormatException e) {
            throw new DslParseException("Invalid integer literal: " + text, e);
        }
    }

    /**
     * Extracts the text content from a {@code mapKey} parser rule context.
     * Handles NAME_IDENTIFIER, QUOTED_STRING, and IN keyword (as literal text).
     *
     * @param ctx The mapKey context from the parser
     * @return The parsed key string
     */
    public static String parseMapKey(ConditionParser.MapKeyContext ctx) {
        if (ctx.NAME_IDENTIFIER() != null) {
            return ctx.NAME_IDENTIFIER().getText();
        }
        if (ctx.QUOTED_STRING() != null) {
            return unquote(ctx.QUOTED_STRING().getText());
        }
        if (ctx.IN() != null) {
            return ctx.IN().getText();
        }
        throw new DslParseException("Could not parse mapKey from ctx: %s".formatted(ctx.getText()));
    }

    /**
     * Extracts a typed value from a {@code valueIdentifier} parser rule context.
     * Handles NAME_IDENTIFIER, QUOTED_STRING, IN keyword (as literal text), and signedInt.
     *
     * @param ctx The valueIdentifier context from the parser
     * @return The parsed value as String or Integer
     */
    public static Object parseValueIdentifier(ConditionParser.ValueIdentifierContext ctx) {
        if (ctx.NAME_IDENTIFIER() != null) {
            return ctx.NAME_IDENTIFIER().getText();
        }
        if (ctx.QUOTED_STRING() != null) {
            return unquote(ctx.QUOTED_STRING().getText());
        }
        if (ctx.IN() != null) {
            return ctx.IN().getText();
        }
        if (ctx.signedInt() != null) {
            return parseSignedInt(ctx.signedInt());
        }
        throw new DslParseException("Could not parse valueIdentifier from ctx: %s".formatted(ctx.getText()));
    }

    /**
     * Get the string inside the quotes.
     *
     * @param str String input
     * @return String inside the quotes
     */
    public static String unquote(String str) {
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

    /**
     * Extracts the type string from a method name expected to start with "as" and end with "()".
     *
     * @param methodName The method name string
     * @return The extracted type string
     * @throws DslParseException if the method name is not in the correct format
     */
    public static String extractTypeFromMethod(String methodName) {
        if (methodName.startsWith("as") && methodName.endsWith("()")) {
            return methodName.substring(2, methodName.length() - 2);
        } else {
            throw new DslParseException("Invalid method name: %s".formatted(methodName));
        }
    }

    /**
     * Extracts the function name from a string that may include parameters in parentheses.
     *
     * @param text The input string containing the function name and potentially parameters
     * @return The extracted function name
     */
    public static String extractFunctionName(String text) {
        int startParen = text.indexOf('(');
        return (startParen != -1) ? text.substring(0, startParen) : text;
    }

    /**
     * Extracts an integer parameter from a string enclosed in parentheses.
     *
     * @param text The input string
     * @return The extracted integer parameter, or {@code null} if not found or invalid
     */
    public static Integer extractParameter(String text) {
        int startParen = text.indexOf('(');
        int endParen = text.indexOf(')');

        if (startParen != -1 && endParen != -1 && endParen > startParen + 1) {
            String numberStr = text.substring(startParen + 1, endParen);
            return Integer.parseInt(numberStr);
        }
        return null;
    }
}
