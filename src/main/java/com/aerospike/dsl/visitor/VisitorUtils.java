package com.aerospike.dsl.visitor;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.exception.NoApplicableFilterException;
import com.aerospike.dsl.index.Index;
import com.aerospike.dsl.model.*;
import lombok.experimental.UtilityClass;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.aerospike.dsl.model.AbstractPart.PartType.*;
import static com.aerospike.dsl.model.Expr.ExprPartsOperation.*;
import static com.aerospike.dsl.util.ValidationUtils.validateComparableTypes;
import static com.aerospike.dsl.visitor.VisitorUtils.ArithmeticTermType.*;

@UtilityClass
public class VisitorUtils {

    public final String INDEX_NAME_SEPARATOR = "||||";
    private final Map<Exp.Type, IndexType> expTypeToIndexType = Map.of(
            Exp.Type.INT, IndexType.NUMERIC,
            Exp.Type.STRING, IndexType.STRING,
            Exp.Type.BLOB, IndexType.BLOB
    );
    private final List<Expr.ExprPartsOperation> CTRL_STRUCTURE_HOLDERS = List.of(
            WITH_STRUCTURE_HOLDER,
            WHEN_STRUCTURE_HOLDER,
            EXCLUSIVE_STRUCTURE_HOLDER
    );


    protected enum FilterOperationType {
        GT,
        GTEQ,
        LT,
        LTEQ,
        EQ,
        NOTEQ
    }

    protected enum ArithmeticTermType {
        ADDEND,
        SUBTR,
        MIN,
        DIFFERENCE,
        DIVIDEND,
        DIVISOR,
        QUOTIENT,
        MULTIPLICAND,
        MULTIPLIER,
        PRODUCT
    }

    static String extractVariableName(String variableReference) {
        if (variableReference.startsWith("${") && variableReference.endsWith("}")) {
            return variableReference.substring(2, variableReference.length() - 1);
        }
        throw new IllegalArgumentException("Input string is not in the correct format");

    }

    static Exp.Type detectImplicitTypeFromUpperTree(ParseTree ctx) {
        // Search for a "leaf" operand child (Int, Float, String and Boolean)
        // in the above levels of the current path in the expression tree
        while (ctx.getParent() != null) {
            ctx = ctx.getParent();

            for (int i = 0; i < ctx.getChildCount(); i++) {
                ParseTree child = ctx.getChild(i);

                if (child instanceof ConditionParser.OperandContext operandContext) {
                    if (operandContext.numberOperand() != null) {
                        if (operandContext.numberOperand().intOperand() != null) {
                            return Exp.Type.INT;
                        } else if (operandContext.numberOperand().floatOperand() != null) {
                            return Exp.Type.FLOAT;
                        }
                    } else if (operandContext.stringOperand() != null) {
                        return Exp.Type.STRING;
                    } else if (operandContext.booleanOperand() != null) {
                        return Exp.Type.BOOL;
                    }
                }
            }
        }
        // Could not detect, return null and determine defaults later on
        return null;
    }

    static void logicalSetBinsAsBooleanExpr(Expr left, Expr right) {
        logicalSetBinAsBooleanExpr(left);
        logicalSetBinAsBooleanExpr(right);
    }

    static void logicalSetBinAsBooleanExpr(Expr expr) {
        if (expr instanceof BinPart) {
            ((BinPart) expr).updateExp(Exp.Type.BOOL);
        }
    }

    static boolean shouldVisitListElement(int i, int size, ParseTree child) {
        return size > 0 // size is not 0
                && i != 0 // not the first element ('[')
                && i != size - 1 // not the last element (']')
                && !child.getText().equals(","); // not a comma (list elements separator)
    }

    static boolean shouldVisitMapElement(int i, int size, ParseTree child) {
        return size > 0 // size is not 0
                && i != 0 // not the first element ('{')
                && i != size - 1 // not the last element ('}')
                && !child.getText().equals(":") // not a colon (map key and value separator)
                && !child.getText().equals(","); // not a comma (map pairs separator)
    }

    // 2 operands Expressions
    static Exp exprToExp(Expr expr) {
        if (expr.isUnary()) {
            return getUnaryExpOrFail(expr.getLeft(), getUnaryExpOperator(expr.getOperationType()));
        }

        AbstractPart left = expr.getLeft();
        AbstractPart right = expr.getRight();
        if (left == null) {
            throw new AerospikeDSLException("Unable to parse left operand");
        }
        if (right == null) {
            throw new AerospikeDSLException("Unable to parse right operand");
        }

        Exp leftExp = getFilterExpression(expr, left, right.getPartType());
        Exp rightExp = getFilterExpression(expr, right, left.getPartType());

        BinaryOperator<Exp> operator = getExpOperator(expr.getOperationType());
        if (left.getPartType() == BIN_PART) {
            return getExpLeftBinTypeComparison((BinPart) left, right, operator);
        }
        if (right.getPartType() == BIN_PART) {
            return getExpRightBinTypeComparison(left, (BinPart) right, operator);
        }
        return operator.apply(leftExp, rightExp);
    }

    private static Exp getFilterExpression(Expr expr, AbstractPart part, AbstractPart.PartType secondPartType) {
        Exp leftExp = null;
        if (part.getPartType() == EXPR) {
            if (expr.isUnary()) {
                if (!CTRL_STRUCTURE_HOLDERS.contains(expr.getOperationType())) {
                    // If the expression holds a control structure
                    leftExp = getUnaryExpOrFail(part, getUnaryExpOperator(expr.getOperationType()));
                    part.setExp(leftExp);
                } else {
                    leftExp = getFilterExp((Expr) part);
                    part.setExp(leftExp);
                }
            } else {
                leftExp = getFilterExp((Expr) part);
                part.setExp(leftExp);
            }
        } else if (part.getPartType() != BIN_PART && secondPartType != BIN_PART) {
            leftExp = part.getExp();
        }
        return leftExp;
    }

    static Exp getExpLeftBinTypeComparison(BinPart left, AbstractPart right, BinaryOperator<Exp> operator) {
        Exp leftExp = Exp.bin(left.getBinName(), left.getExpType());
        return switch (right.getPartType()) {
            case INT_OPERAND -> {
                validateComparableTypes(left.getExpType(), Exp.Type.INT);
                yield operator.apply(leftExp, right.getExp());
            }
            case FLOAT_OPERAND -> {
                validateComparableTypes(left.getExpType(), Exp.Type.FLOAT);
                yield operator.apply(leftExp, right.getExp());
            }
            case BOOL_OPERAND -> {
                validateComparableTypes(left.getExpType(), Exp.Type.BOOL);
                yield operator.apply(leftExp, right.getExp());
            }
            case STRING_OPERAND -> {
                if (left.getExpType() != null &&
                        left.getExpType().equals(Exp.Type.BLOB)) {
                    // Base64 Blob
                    validateComparableTypes(left.getExpType(), Exp.Type.BLOB);
                    StringOperand stringRight = (StringOperand) right;
                    stringRight.setBlob(true);
                    yield operator.apply(leftExp, stringRight.getExp());
                } else {
                    // String
                    validateComparableTypes(left.getExpType(), Exp.Type.STRING);
                    yield operator.apply(leftExp, right.getExp());
                }
            }
            case METADATA_OPERAND -> {
                // No need to validate, types are determined by metadata function
                Exp.Type binType = Exp.Type.valueOf(((MetadataOperand) right).getMetadataType().toString());
                leftExp = Exp.bin(left.getBinName(), binType);
                yield operator.apply(leftExp, right.getExp());
            }
            case EXPR, PATH_OPERAND -> operator.apply(leftExp, right.getExp()); // Can't validate with Expr on one side
            case BIN_PART -> {
                // Left and right are both bin parts
                // Validate types if possible
                validateComparableTypes(left.getExpType(), right.getExpType());
                yield operator.apply(leftExp, right.getExp());
            }
            case LIST_OPERAND -> {
                validateComparableTypes(left.getExpType(), Exp.Type.LIST);
                yield operator.apply(leftExp, right.getExp());
            }
            case MAP_OPERAND -> {
                validateComparableTypes(left.getExpType(), Exp.Type.MAP);
                yield operator.apply(leftExp, right.getExp());
            }
            case VARIABLE_OPERAND -> operator.apply(leftExp, right.getExp());
            default -> throw new AerospikeDSLException("Operand type not supported: %s".formatted(right.getPartType()));
        };
    }

    static Exp getExpRightBinTypeComparison(AbstractPart left, BinPart right, BinaryOperator<Exp> operator) {
        Exp rightExp = Exp.bin(right.getBinName(), right.getExpType());
        return switch (left.getPartType()) {
            case INT_OPERAND -> {
                validateComparableTypes(Exp.Type.INT, right.getExpType());
                yield operator.apply(left.getExp(), rightExp);
            }
            case FLOAT_OPERAND -> {
                validateComparableTypes(Exp.Type.FLOAT, right.getExpType());
                yield operator.apply(left.getExp(), rightExp);
            }
            case BOOL_OPERAND -> {
                validateComparableTypes(Exp.Type.BOOL, right.getExpType());
                yield operator.apply(left.getExp(), rightExp);
            }
            case STRING_OPERAND -> {
                if (right.getExpType() != null &&
                        right.getExpType().equals(Exp.Type.BLOB)) {
                    // Base64 Blob
                    validateComparableTypes(Exp.Type.BLOB, right.getExpType());
                    StringOperand stringLeft = (StringOperand) left;
                    stringLeft.setBlob(true);
                    yield operator.apply(stringLeft.getExp(), rightExp);
                } else {
                    // String
                    validateComparableTypes(Exp.Type.STRING, right.getExpType());
                    yield operator.apply(left.getExp(), rightExp);
                }
            }
            case METADATA_OPERAND -> {
                // No need to validate, types are determined by metadata function
                Exp.Type binType = Exp.Type.valueOf(((MetadataOperand) left).getMetadataType().toString());
                rightExp = Exp.bin(right.getBinName(), binType);
                yield operator.apply(left.getExp(), rightExp);
            }
            case EXPR, PATH_OPERAND ->
                    operator.apply(left.getExp(), right.getExp()); // Can't validate with Expr on one side
            // No need for 2 BIN_OPERAND handling since it's covered in the left condition
            case LIST_OPERAND -> {
                validateComparableTypes(Exp.Type.LIST, right.getExpType());
                yield operator.apply(left.getExp(), rightExp);
            }
            case MAP_OPERAND -> {
                validateComparableTypes(Exp.Type.MAP, right.getExpType());
                yield operator.apply(left.getExp(), rightExp);
            }
            case VARIABLE_OPERAND -> operator.apply(left.getExp(), rightExp);
            default -> throw new AerospikeDSLException("Operand type not supported: %s".formatted(left.getPartType()));
        };
    }

    // 1 operand Expressions
    static Exp getUnaryExpOrFail(AbstractPart operand, UnaryOperator<Exp> operator) {
        if (operand == null) {
            throw new AerospikeDSLException("Unable to parse unary operand");
        }

        return switch (operand.getPartType()) {
            case BIN_PART -> {
                BinPart binPart = ((BinPart) operand);
                // There is only 1 case of a single bin expression
                yield operator.apply(Exp.bin(binPart.getBinName(), binPart.getExpType()));
            }
            case METADATA_OPERAND -> {
                MetadataOperand metadataOperand = ((MetadataOperand) operand);
                yield operator.apply(metadataOperand.getExp());
            }
            default -> throw new AerospikeDSLException("Unsupported part type for an unary expression");
        };
    }

    static String getPathFunctionParam(ConditionParser.PathFunctionParamContext paramCtx, String paramName) {
        String paramNameText;
        String paramNameValue;
        String paramValue = null;
        if (paramCtx.pathFunctionParamName() != null) {
            paramNameText = paramCtx.pathFunctionParamName().getText();
            paramNameValue = paramCtx.pathFunctionParamValue().getText();
            if (paramNameText.equalsIgnoreCase(paramName)) {
                paramValue = paramNameValue;
            }
        }
        return paramValue;
    }

    static String extractTypeFromMethod(String methodName) {
        if (methodName.startsWith("as") && methodName.endsWith("()")) {
            return methodName.substring(2, methodName.length() - 2);
        } else {
            throw new AerospikeDSLException("Invalid method name: %s".formatted(methodName));
        }
    }

    static String extractFunctionName(String text) {
        int startParen = text.indexOf('(');
        return (startParen != -1) ? text.substring(0, startParen) : text;
    }

    static Integer extractParameter(String text) {
        int startParen = text.indexOf('(');
        int endParen = text.indexOf(')');

        if (startParen != -1 && endParen != -1 && endParen > startParen + 1) {
            String numberStr = text.substring(startParen + 1, endParen);
            return Integer.parseInt(numberStr);
        }
        return null;
    }

    // 2 operands Filters
    static Filter getFilterOrFail(AbstractPart left, AbstractPart right, FilterOperationType type) {
        if (left == null) {
            throw new AerospikeDSLException("Unable to parse left operand");
        }
        if (right == null) {
            throw new AerospikeDSLException("Unable to parse right operand");
        }

        if (left.getPartType() == BIN_PART) {
            return getFilter((BinPart) left, right, type);
        }
        if (right.getPartType() == BIN_PART) {
            return getFilter((BinPart) right, left, invertType(type));
        }

        // Handle non Bin operands cases
        if (left instanceof Expr leftExpr) {
            return getFilterOrFail(leftExpr.getLeft(), leftExpr.getRight(), leftExpr.getOperationType(), right, type);
        }
        if (right instanceof Expr rightExpr) {
            return getFilterOrFail(rightExpr.getLeft(), rightExpr.getRight(), rightExpr.getOperationType(), left, type);
        }
        return null;
    }

    // 2 operands Filters
    static Filter getFilterOrFail(AbstractPart exprLeft, AbstractPart exprRight, Expr.ExprPartsOperation operationType,
                                  AbstractPart right, FilterOperationType type) {
        if (exprLeft == null) {
            throw new AerospikeDSLException("Unable to parse left operand of expression");
        }
        if (exprRight == null) {
            throw new AerospikeDSLException("Unable to parse right operand of expression");
        }

        if (exprLeft.getPartType() == BIN_PART) { // bin is on the left side
            if (exprRight instanceof IntOperand leftOperand && right instanceof IntOperand rightOperand) {
                validateComparableTypes(exprLeft.getExpType(), Exp.Type.INT);
                return applyFilterOperator(((BinPart) exprLeft).getBinName(), leftOperand, rightOperand,
                        operationType, type, getTermType(operationType, true));
            }
            throw new AerospikeDSLException(
                    String.format("Operands not supported in secondary index Filter: %s, %s", exprRight, right));
        }
        if (exprRight.getPartType() == BIN_PART) { // bin is on the right side
            if (exprLeft instanceof IntOperand leftOperand && right instanceof IntOperand rightOperand) {
                validateComparableTypes(exprRight.getExpType(), Exp.Type.INT);
                return applyFilterOperator(((BinPart) exprRight).getBinName(), leftOperand, rightOperand,
                        operationType, type, getTermType(operationType, false));
            }
            throw new AerospikeDSLException(
                    String.format("Operands not supported in secondary index Filter: %s, %s", exprRight, right));
        }

        // Handle non Bin operands cases
        if (exprLeft instanceof Expr leftExpr) {
            return getFilterOrFail(leftExpr.getLeft(), leftExpr.getRight(), type);
        }
        return null;
    }

    static void validateNumericBinForFilter(AbstractPart left, AbstractPart right) {
        if (!isNumericBin(left, right)) {
            throw new NoApplicableFilterException("The operation is not supported by secondary index filter");
        }
    }

    private static boolean isNumericBin(AbstractPart left, AbstractPart right) {
        return (left.getPartType() == BIN_PART && right.getPartType() == INT_OPERAND)
                || (right.getPartType() == BIN_PART && left.getPartType() == INT_OPERAND);
    }

    private static ArithmeticTermType getTermType(Expr.ExprPartsOperation operationType, boolean isLeftTerm) {
        return switch (operationType) {
            case ADD -> ADDEND;
            case SUB -> isLeftTerm ? SUBTR : MIN;
            case DIV -> isLeftTerm ? DIVIDEND : DIVISOR;
            case MUL -> isLeftTerm ? MULTIPLICAND : MULTIPLIER;
            default -> throw new UnsupportedOperationException("Not supported: " + operationType);
        };
    }

    private static Pair<Long, Long> getLimitsForDivisionForFilter(long left, long right, FilterOperationType type,
                                                                  ArithmeticTermType termType) {
        // Prevent division by zero
        if (right == 0) {
            throw new NoApplicableFilterException("Cannot divide by zero");
        }

        return switch (termType) {
            case DIVIDEND -> LimitsForBinDividend(left, right, type);
            case DIVISOR -> getLimitsForBinDivisor(left, right, type);
            default -> throw new UnsupportedOperationException("Unsupported term type for division: " + termType);
        };
    }

    private static Pair<Long, Long> LimitsForBinDividend(long left, long right,
                                                         FilterOperationType operationType) {
        if (left > 0 && right > 0) {
            // both operands are positive
            return getLimitsForBinDividendWithLeftNumberPositive(operationType, left, right);
        } else if (left == 0 && right == 0) {
            throw new AerospikeDSLException("Undefined division for 0 / 0");
        } else if (left < 0 && right < 0) {
            // both operands are negative
            return getLimitsForBinDividendWithLeftNumberNegative(operationType, left, right);
        } else if (left > 0 && right < 0) {
            // left positive, right negative
            return getLimitsForBinDividendWithLeftNumberPositive(operationType, left, right);
        } else if (right > 0 && left < 0) {
            // left negative, right positive
            return getLimitsForBinDividendWithLeftNumberNegative(operationType, left, right);
        } else if (left != 0) {
            throw new AerospikeDSLException("Division by zero is not allowed");
        } else {
            return new Pair<>(null, null);
        }
    }

    private static Pair<Long, Long> getLimitsForBinDividendWithLeftNumberNegative(FilterOperationType operationType,
                                                                                  long left, long right) {
        return switch (operationType) {
            case GT:
                yield new Pair<>(Long.MIN_VALUE, left * right - 1);
            case GTEQ:
                yield new Pair<>(Long.MIN_VALUE, left * right);
            case LT:
                yield new Pair<>(left * right + 1, Long.MAX_VALUE);
            case LTEQ:
                yield new Pair<>(left * right, Long.MAX_VALUE);
            default:
                throw new AerospikeDSLException("OperationType not supported for division: " + operationType);
        };
    }

    private static Pair<Long, Long> getLimitsForBinDividendWithLeftNumberPositive(FilterOperationType operationType,
                                                                                  long left, long right) {
        return switch (operationType) {
            case GT:
                yield new Pair<>(left * right + 1, Long.MAX_VALUE);
            case GTEQ:
                yield new Pair<>(left * right, Long.MAX_VALUE);
            case LT:
                yield new Pair<>(Long.MIN_VALUE, left * right - 1);
            case LTEQ:
                yield new Pair<>(Long.MIN_VALUE, left * right);
            default:
                throw new AerospikeDSLException("OperationType not supported for division: " + operationType);
        };
    }

    private static Pair<Long, Long> getLimitsForBinDivisor(long left, long right, FilterOperationType operationType) {
        if (left > 0 && right > 0) {
            // both operands are positive
            return switch (operationType) {
                case GT:
                    yield new Pair<>(1L, getClosestLongToTheLeft((float) left / right));
                case GTEQ:
                    yield new Pair<>(1L, left / right);
                case LT, LTEQ:
                    yield new Pair<>(null, null);
                default:
                    throw new AerospikeDSLException("OperationType not supported for division: " + operationType);
            };
        } else if (left == 0 && right == 0) {
            throw new AerospikeDSLException("Cannot divide by zero");
        } else if (left < 0 && right < 0) {
            // both operands are negative
            return switch (operationType) {
                case GT, GTEQ:
                    yield new Pair<>(null, null);
                case LT:
                    yield new Pair<>(1L, getClosestLongToTheLeft((float) left / right));
                case LTEQ:
                    yield new Pair<>(1L, left / right);
                default:
                    throw new AerospikeDSLException("OperationType not supported for division: " + operationType);
            };
        } else if (left > 0 && right < 0) {
            // left positive, right negative
            return switch (operationType) {
                case GT, GTEQ:
                    yield new Pair<>(null, null);
                case LT:
                    yield new Pair<>(getClosestLongToTheRight((float) left / right), -1L);
                case LTEQ:
                    yield new Pair<>(left / right, -1L);
                default:
                    throw new AerospikeDSLException("OperationType not supported for division: " + operationType);
            };
        } else if (right > 0 && left < 0) {
            // right positive, left negative
            return switch (operationType) {
                case GT:
                    yield new Pair<>(getClosestLongToTheRight((float) left / right), -1L);
                case GTEQ:
                    yield new Pair<>(left / right, -1L);
                case LT, LTEQ:
                    yield new Pair<>(null, null);
                default:
                    throw new AerospikeDSLException("OperationType not supported for division: " + operationType);
            };
        } else if (left != 0) {
            throw new AerospikeDSLException("Division by zero is not allowed");
        } else {
            return new Pair<>(null, null);
        }
    }

    private static Filter getFilterForDivOrFail(String binName, Pair<Long, Long> value, FilterOperationType type) {
        // Based on the operation type, generate the appropriate filter range
        return switch (type) {
            case GT, GTEQ, LT, LTEQ -> Filter.range(binName, value.a, value.b);  // Range from 1 to value - 1
            case EQ -> Filter.equal(binName, value.a);  // Exact match for equality case
            default -> throw new AerospikeDSLException("OperationType not supported for division: " + type);
        };
    }

    private static Filter getFilter(BinPart bin, AbstractPart operand, FilterOperationType type) {
        String binName = bin.getBinName();
        return switch (operand.getPartType()) {
            case INT_OPERAND -> {
                validateComparableTypes(bin.getExpType(), Exp.Type.INT);
                yield getFilterForArithmeticOrFail(binName, ((IntOperand) operand).getValue(), type);

            }
            case STRING_OPERAND -> {
                if (type != FilterOperationType.EQ) throw new NoApplicableFilterException("Operand type not supported");

                if (bin.getExpType() != null &&
                        bin.getExpType().equals(Exp.Type.BLOB)) {
                    // Base64 Blob
                    validateComparableTypes(bin.getExpType(), Exp.Type.BLOB);
                    String base64String = ((StringOperand) operand).getValue();
                    byte[] value = Base64.getDecoder().decode(base64String);
                    yield Filter.equal(binName, value);
                } else {
                    // String
                    validateComparableTypes(bin.getExpType(), Exp.Type.STRING);
                    yield Filter.equal(binName, ((StringOperand) operand).getValue());
                }
            }
            default ->
                    throw new NoApplicableFilterException("Operand type not supported: %s".formatted(operand.getPartType()));
        };
    }

    private static Filter applyFilterOperator(String binName, IntOperand leftOperand, IntOperand rightOperand,
                                              Expr.ExprPartsOperation operationType, FilterOperationType type,
                                              ArithmeticTermType termType) {
        long leftValue = leftOperand.getValue();
        long rightValue = rightOperand.getValue();
        float value;
        if (Objects.requireNonNull(operationType) == ADD) {
            value = rightValue - leftValue;
        } else if (operationType == SUB) {
            value = switch (termType) {
                case SUBTR -> rightValue + leftValue;
                case MIN -> {
                    type = invertType(type);
                    yield leftValue - rightValue;
                }
                default -> throw new IllegalStateException("Unexpected term type: " + termType);
            };
        } else if (operationType == DIV) {
            Pair<Long, Long> valueForDiv = getLimitsForDivisionForFilter(leftValue, rightValue, type, termType);
            if (valueForDiv.a == null
                    || valueForDiv.b == null
                    || valueForDiv.a > valueForDiv.b
                    || (valueForDiv.a == 0 && valueForDiv.b == 0)) {
                throw new NoApplicableFilterException("The operation is not supported by secondary index filter");
            }
            return getFilterForDivOrFail(binName, valueForDiv, type);
        } else if (operationType == MUL) {
            if (leftValue <= 0) {
                if (leftValue == 0) throw new NoApplicableFilterException("Cannot divide by zero");
                type = invertType(type);
            }
            float val = (float) rightValue / leftValue;
            return getFilterForArithmeticOrFail(binName, val, type);
        } else {
            throw new UnsupportedOperationException("Not supported");
        }
        return getFilterForArithmeticOrFail(binName, value, type);
    }

    private static Filter getFilterForArithmeticOrFail(String binName, float value, FilterOperationType type) {
        return switch (type) {
            // "$.intBin1 > 100" and "100 < $.intBin1" represent the same Filter
            case GT -> Filter.range(binName, getClosestLongToTheRight(value), Long.MAX_VALUE);
            case GTEQ -> Filter.range(binName, (long) value, Long.MAX_VALUE);
            case LT -> Filter.range(binName, Long.MIN_VALUE, getClosestLongToTheLeft(value));
            case LTEQ -> Filter.range(binName, Long.MIN_VALUE, (long) value);
            case EQ -> Filter.equal(binName, (long) value);
            default ->
                    throw new NoApplicableFilterException("The operation is not supported by secondary index filter");
        };
    }

    private static long getClosestLongToTheLeft(float value) {
        // Get the largest integer less than or equal to the float
        long flooredValue = (long) Math.floor(value);

        // If the float is a round number, subtract 1
        if (value == flooredValue) {
            return flooredValue - 1;
        }

        return flooredValue;
    }

    private static long getClosestLongToTheRight(float value) {
        // Get the smallest integer greater than or equal to the float
        long ceiledValue = (long) Math.ceil(value);

        // If the float is a round number, add 1
        if (value == ceiledValue) {
            return ceiledValue + 1;
        }

        return ceiledValue;
    }

    private FilterOperationType invertType(FilterOperationType type) {
        return switch (type) {
            case GT -> FilterOperationType.LT;
            case GTEQ -> FilterOperationType.LTEQ;
            case LT -> FilterOperationType.GT;
            case LTEQ -> FilterOperationType.GTEQ;
            default -> type;
        };
    }

    public AbstractPart buildExpr(Expr expr, String namespace, Map<String, Index> indexes,
                                  boolean isFilterExpOnly, boolean isSIFilterOnly) {
        Exp exp = null;
        Filter sIndexFilter = null;
        try {
            if (!isFilterExpOnly) {
                sIndexFilter = getSIFilter(expr, namespace, indexes);
            }
        } catch (NoApplicableFilterException ignored) {
        }
        expr.setSIndexFilter(sIndexFilter);

        if (!isSIFilterOnly) {
            exp = getFilterExp(expr);
        }
        expr.setExp(exp);
        return expr;
    }

    private static Exp getFilterExp(Expr expr) {
        // if a Filter is already set in an OR query
        if (expr.getOperationType() == OR && expr.getSIndexFilter() != null) return null;

        return switch (expr.getOperationType()) {
            case WITH_STRUCTURE_HOLDER -> withStructureHolderToExp(expr);
            case WHEN_STRUCTURE_HOLDER -> whenStructureHolderToExp(expr);
            case EXCLUSIVE_STRUCTURE_HOLDER -> exclStructureHolderToExp(expr);
            default -> exprToExp(expr);
        };
    }

    private static Exp withStructureHolderToExp(Expr expr) {
        List<Exp> expressions = new ArrayList<>();
        WithStructure withOperandsList = (WithStructure) expr.getLeft(); // extract unary Expr operand
        List<WithOperand> operands = withOperandsList.getOperands();
        for (WithOperand withOperand : operands) {
            if (!withOperand.isLastPart()) {
                expressions.add(Exp.def(withOperand.getString(), getExp(withOperand.getPart())));
            } else {
                // the last expression is the action (described after "do")
                expressions.add(getExp(withOperand.getPart()));
            }
        }
        return Exp.let(expressions.toArray(new Exp[0]));
    }

    private static Exp whenStructureHolderToExp(Expr expr) {
        List<Exp> expressions = new ArrayList<>();
        WhenStructure whenOperandsList = (WhenStructure) expr.getLeft(); // extract unary Expr operand
        List<AbstractPart> operands = whenOperandsList.getOperands();
        for (AbstractPart part : operands) {
            expressions.add(getExp(part));
        }
        return Exp.cond(expressions.toArray(new Exp[0]));
    }

    private static Exp exclStructureHolderToExp(Expr expr) {
        List<Exp> expressions = new ArrayList<>();
        ExclusiveStructure whenOperandsList = (ExclusiveStructure) expr.getLeft(); // extract unary Expr operand
        List<Expr> operands = whenOperandsList.getOperands();
        for (Expr part : operands) {
            expressions.add(getExp(part));
        }
        return Exp.exclusive(expressions.toArray(new Exp[0]));
    }

    private static Exp getExp(AbstractPart part) {
        if (part.getPartType() == EXPR) {
            return getFilterExp((Expr) part);
        }
        return part.getExp();
    }

    private static Filter getSIFilter(Expr expr, String namespace, Map<String, Index> indexes) {
        List<Expr> exprs = flattenExprs(expr);
        Expr chosenExpr = chooseExprForFilter(exprs, namespace, indexes);
        return chosenExpr == null ? null
                : getFilterOrFail(chosenExpr.getLeft(),
                chosenExpr.getRight(),
                getFilterOperation(chosenExpr.getOperationType())
        );
    }

    private static Expr chooseExprForFilter(List<Expr> exprs, String namespace, Map<String, Index> indexes) {
        if (exprs.size() == 1) return exprs.get(0);
        if (exprs.size() > 1 && (indexes == null || indexes.isEmpty())) return null;

        Map<Integer, List<Expr>> exprsPerCardinality = new HashMap<>();
        for (Expr expr : exprs) {
            BinPart binPart = getBinPart(expr);
                Index index = indexes.get(namespace + INDEX_NAME_SEPARATOR + binPart.getBinName());
                if (index == null) continue;
                if (expTypeToIndexType.get(binPart.getExpType()) == index.getIndexType()) {
                    List<Expr> exprsList = exprsPerCardinality.get(index.getBinValuesRatio());
                    if (exprsList != null) {
                        exprsList.add(expr);
                    } else {
                        exprsList = new ArrayList<>();
                        exprsList.add(expr);
                    }
                    exprsPerCardinality.put(index.getBinValuesRatio(), exprsList);
                }
        }

        // Find the entry with the largest key and put it in a new Map
        Map<Integer, List<Expr>> largestCardinalityMap = exprsPerCardinality.entrySet().stream()
                .max(Map.Entry.comparingByKey())
                .map(entry -> Map.of(entry.getKey(), entry.getValue()))
                .orElse(Collections.emptyMap());
        List<Expr> largestCardinalityExprs = largestCardinalityMap.values().iterator().next();
        if (largestCardinalityExprs.size() > 1) {
            // Choosing alphabetically
            return largestCardinalityExprs.stream()
                    .min(Comparator.comparing(expr -> getBinPart(expr).getBinName()))
                    .orElse(null);
        }
        return largestCardinalityExprs.get(0);
    }

    private static BinPart getBinPart(Expr expr) {
        BinPart result = null;
        if (expr.getOperationType() == AND || expr.getOperationType() == OR) {
            Expr leftExpr = (Expr) expr.getLeft();
            if (leftExpr.getLeft() != null && leftExpr.getLeft().getPartType() == EXPR) {
                result = getBinPart(leftExpr);
                if (result != null) return result;
            }

            if (expr.getRight() != null && expr.getRight().getPartType() == EXPR) {
                Expr rightExpr = (Expr) expr.getRight();
                if (rightExpr.getRight() != null && rightExpr.getRight().getPartType() == EXPR) {
                    result = getBinPart(rightExpr);
                    if (result != null) return result;
                }
            }
        } else {
            if (expr.getLeft() != null && expr.getLeft().getPartType() == BIN_PART) {
                return (BinPart) expr.getLeft();
            }

            if (expr.getRight() != null && expr.getRight().getPartType() == BIN_PART) {
                return (BinPart) expr.getRight();
            }
        }
        return result;
    }

    private static List<Expr> flattenExprs(Expr expr) {
        List<Expr> results = new ArrayList<>();
        if (expr.getOperationType() == AND || expr.getOperationType() == OR) {
            if (expr.getLeft() != null && expr.getLeft().getPartType() == EXPR) {
                Expr leftExpr = (Expr) expr.getLeft();
                if (leftExpr.getOperationType() == AND || leftExpr.getOperationType() == OR) {
                    Stream<Expr> stream = flattenExprs(leftExpr).stream();
                    results = Stream.concat(results.stream(), stream).toList();
                } else {
                    Stream<Expr> stream = Stream.of(leftExpr);
                    results = Stream.concat(results.stream(), stream).toList();
                }
            }

            if (expr.getRight() != null && expr.getRight().getPartType() == EXPR) {
                Expr rightExpr = (Expr) expr.getRight();
                if (rightExpr.getOperationType() == AND || rightExpr.getOperationType() == OR) {
                    Stream<Expr> stream = flattenExprs(rightExpr).stream();
                    results = Stream.concat(results.stream(), stream).toList();
                } else {
                    Stream<Expr> stream = Stream.of(rightExpr);
                    results = Stream.concat(results.stream(), stream).toList();
                }
            }
        } else {
            return List.of(expr);
        }
        return results;
    }

    private static FilterOperationType getFilterOperation(Expr.ExprPartsOperation exprPartsOperation) {
        return switch (exprPartsOperation) {
            case GT -> FilterOperationType.GT;
            case GTEQ -> FilterOperationType.GTEQ;
            case LT -> FilterOperationType.LT;
            case LTEQ -> FilterOperationType.LTEQ;
            case EQ -> FilterOperationType.EQ;
            case NOTEQ -> FilterOperationType.NOTEQ;
            default -> throw new NoApplicableFilterException("ExprPartsOperation has no matching FilterOperationType");
        };
    }

    private static BinaryOperator<Exp> getExpOperator(Expr.ExprPartsOperation exprPartsOperation) {
        return switch (exprPartsOperation) {
            case ADD -> Exp::add;
            case SUB -> Exp::sub;
            case MUL -> Exp::mul;
            case DIV -> Exp::div;
            case MOD -> Exp::mod;
            case INT_XOR -> Exp::intXor;
            case L_SHIFT -> Exp::lshift;
            case R_SHIFT -> Exp::rshift;
            case INT_AND -> Exp::intAnd;
            case INT_OR -> Exp::intOr;
            case AND -> Exp::and;
            case OR -> Exp::or;
            case EQ -> Exp::eq;
            case NOTEQ -> Exp::ne;
            case LT -> Exp::lt;
            case LTEQ -> Exp::le;
            case GT -> Exp::gt;
            case GTEQ -> Exp::ge;
            default -> throw new NoApplicableFilterException("ExprPartsOperation has no matching BinaryOperator<Exp>");
        };
    }

    private static UnaryOperator<Exp> getUnaryExpOperator(Expr.ExprPartsOperation exprPartsOperation) {
        return switch (exprPartsOperation) {
            case INT_NOT -> Exp::intNot;
            case NOT -> Exp::not;
            default -> throw new NoApplicableFilterException("ExprPartsOperation has no matching UnaryOperator<Exp>");
        };
    }
}
