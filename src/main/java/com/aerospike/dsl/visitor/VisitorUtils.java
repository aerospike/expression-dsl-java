package com.aerospike.dsl.visitor;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.model.BinPart;
import com.aerospike.dsl.model.Expr;
import com.aerospike.dsl.model.IntOperand;
import com.aerospike.dsl.model.MetadataOperand;
import com.aerospike.dsl.model.StringOperand;
import com.aerospike.dsl.util.ValidationUtils;
import lombok.experimental.UtilityClass;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Base64;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

import static com.aerospike.dsl.model.Expr.ExprPartsOperation.*;
import static com.aerospike.dsl.visitor.VisitorUtils.ArithmeticTermType.*;
import static com.aerospike.dsl.visitor.VisitorUtils.FilterOperationType.*;

@UtilityClass
public class VisitorUtils {

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
        } else {
            throw new IllegalArgumentException("Input string is not in the correct format");
        }
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
    static Exp getExpOrFail(AbstractPart left, AbstractPart right, BinaryOperator<Exp> operator) {
        if (left == null) {
            throw new AerospikeDSLException("Unable to parse left operand");
        }
        if (right == null) {
            throw new AerospikeDSLException("Unable to parse right operand");
        }

        if (left.getPartType() == AbstractPart.PartType.BIN_PART) {
            return getExpLeftBinTypeComparison((BinPart) left, right, operator);
        }
        if (right.getPartType() == AbstractPart.PartType.BIN_PART) {
            return getExpRightBinTypeComparison(left, (BinPart) right, operator);
        }

        // Handle non Bin operands cases
        Exp leftExp = left.getExp();
        Exp rightExp = right.getExp();
        return operator.apply(leftExp, rightExp);
    }

    static Exp getExpLeftBinTypeComparison(BinPart left, AbstractPart right, BinaryOperator<Exp> operator) {
        String binNameLeft = left.getBinName();
        return switch (right.getPartType()) {
            case INT_OPERAND -> {
                ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.INT);
                yield operator.apply(left.getExp(), right.getExp());
            }
            case FLOAT_OPERAND -> {
                ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.FLOAT);
                yield operator.apply(left.getExp(), right.getExp());
            }
            case BOOL_OPERAND -> {
                ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.BOOL);
                yield operator.apply(left.getExp(), right.getExp());
            }
            case STRING_OPERAND -> {
                if (left.getExpType() != null &&
                        left.getExpType().equals(Exp.Type.BLOB)) {
                    // Base64 Blob
                    ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.BLOB);
                    String base64String = ((StringOperand) right).getValue();
                    byte[] value = Base64.getDecoder().decode(base64String);
                    yield operator.apply(left.getExp(), Exp.val(value));
                } else {
                    // String
                    ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.STRING);
                    yield operator.apply(left.getExp(), right.getExp());
                }
            }
            case METADATA_OPERAND -> {
                // No need to validate, types are determined by metadata function
                Exp.Type binType = Exp.Type.valueOf(((MetadataOperand) right).getMetadataType().toString());
                yield operator.apply(
                        Exp.bin(binNameLeft, binType),
                        right.getExp()
                );
            }
            case EXPR, PATH_OPERAND ->
                    operator.apply(left.getExp(), right.getExp()); // Can't validate with Expr on one side
            // Left and right are both bin parts
            case BIN_PART -> {
                // Validate types if possible
                ValidationUtils.validateComparableTypes(left.getExpType(), right.getExpType());
                yield operator.apply(left.getExp(), right.getExp());
            }
            case LIST_OPERAND -> {
                ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.LIST);
                yield operator.apply(left.getExp(), right.getExp());
            }
            case MAP_OPERAND -> {
                ValidationUtils.validateComparableTypes(left.getExpType(), Exp.Type.MAP);
                yield operator.apply(left.getExp(), right.getExp());
            }
            default -> throw new AerospikeDSLException("Operand type not supported: %s".formatted(right.getPartType()));
        };
    }

    static Exp getExpRightBinTypeComparison(AbstractPart left, BinPart right, BinaryOperator<Exp> operator) {
        String binNameRight = right.getBinName();
        return switch (left.getPartType()) {
            case INT_OPERAND -> {
                ValidationUtils.validateComparableTypes(Exp.Type.INT, right.getExpType());
                yield operator.apply(left.getExp(), right.getExp());
            }
            case FLOAT_OPERAND -> {
                ValidationUtils.validateComparableTypes(Exp.Type.FLOAT, right.getExpType());
                yield operator.apply(left.getExp(), right.getExp());
            }
            case BOOL_OPERAND -> {
                ValidationUtils.validateComparableTypes(Exp.Type.BOOL, right.getExpType());
                yield operator.apply(left.getExp(), right.getExp());
            }
            case STRING_OPERAND -> {
                if (right.getExpType() != null &&
                        right.getExpType().equals(Exp.Type.BLOB)) {
                    // Base64 Blob
                    ValidationUtils.validateComparableTypes(Exp.Type.BLOB, right.getExpType());
                    String base64String = ((StringOperand) left).getValue();
                    byte[] value = Base64.getDecoder().decode(base64String);
                    yield operator.apply(Exp.val(value), right.getExp());
                } else {
                    // String
                    ValidationUtils.validateComparableTypes(Exp.Type.STRING, right.getExpType());
                    yield operator.apply(left.getExp(), right.getExp());
                }
            }
            case METADATA_OPERAND -> {
                // No need to validate, types are determined by metadata function
                Exp.Type binType = Exp.Type.valueOf(((MetadataOperand) left).getMetadataType().toString());
                yield operator.apply(
                        left.getExp(),
                        Exp.bin(binNameRight, binType)
                );
            }
            case EXPR, PATH_OPERAND ->
                    operator.apply(left.getExp(), right.getExp()); // Can't validate with Expr on one side
            // No need for 2 BIN_OPERAND handling since it's covered in the left condition
            case LIST_OPERAND -> {
                ValidationUtils.validateComparableTypes(Exp.Type.LIST, right.getExpType());
                yield operator.apply(left.getExp(), right.getExp());
            }
            case MAP_OPERAND -> {
                ValidationUtils.validateComparableTypes(Exp.Type.MAP, right.getExpType());
                yield operator.apply(left.getExp(), right.getExp());
            }
            default -> throw new AerospikeDSLException("Operand type not supported: %s".formatted(left.getPartType()));
        };
    }

    // 1 operand Expressions
    static Exp getExpOrFail(AbstractPart operand, UnaryOperator<Exp> operator) {
        if (operand == null) {
            throw new AerospikeDSLException("Unable to parse operand");
        }

        // 1 Operand Expression is always a BIN Operand
        String binName = ((BinPart) operand).getBinName();

        // There is only 1 case of a single operand expression (int not), and it always gets an integer
        return operator.apply(Exp.bin(binName, Exp.Type.INT));
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

        if (left.getPartType() == AbstractPart.PartType.BIN_PART) {
            return getFilter((BinPart) left, right, type);
        }
        if (right.getPartType() == AbstractPart.PartType.BIN_PART) {
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

        if (exprLeft.getPartType() == AbstractPart.PartType.BIN_PART) { // bin is on the left side
            if (exprRight instanceof IntOperand leftOperand && right instanceof IntOperand rightOperand) {
                ValidationUtils.validateComparableTypes(exprLeft.getExpType(), Exp.Type.INT);
                return applyFilterOperator(((BinPart) exprLeft).getBinName(), leftOperand, rightOperand,
                        operationType, type, getTermType(operationType, true));
            }
            throw new AerospikeDSLException("Not supported");
        }
        if (exprRight.getPartType() == AbstractPart.PartType.BIN_PART) { // bin is on the right side
            if (exprLeft instanceof IntOperand leftOperand && right instanceof IntOperand rightOperand) {
                ValidationUtils.validateComparableTypes(exprRight.getExpType(), Exp.Type.INT);
                return applyFilterOperator(((BinPart) exprRight).getBinName(), leftOperand, rightOperand,
                        operationType, type, getTermType(operationType, false));
            }
            throw new AerospikeDSLException("Not supported");
        }

        // Handle non Bin operands cases
        if (exprLeft instanceof Expr leftExpr) {
            return getFilterOrFail(leftExpr.getLeft(), leftExpr.getRight(), type);
        }
//        if (right.getPartType() == AbstractPart.PartType.BIN_PART) {
//            return getFilterRightBinTypeComparison((BinPart) right, left, type);
//        }
        return null;
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

    private static Pair<Long, Long> getLimitsForDivision(long left, long right, FilterOperationType type,
                                                         ArithmeticTermType termType) {
        // Prevent division by zero
        if (right == 0) {
            throw new AerospikeDSLException("Cannot divide by zero");
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
        } else if ((left > 0 && right == 0) || (left < 0 && right == 0)) {
            throw new AerospikeDSLException("Division by zero is not allowed");
        } else if ((left == 0 && right > 0) || ((left == 0 && right < 0))) {
            return new Pair<>(null, null);
        }
        return new Pair<>(null, null);
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
                    yield new Pair<>(1L, left / right - 1L);
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
                    yield new Pair<>(0L, left / right - 1L);
                case LTEQ:
                    yield new Pair<>(0L, left / right);
                default:
                    throw new AerospikeDSLException("OperationType not supported for division: " + operationType);
            };
        } else if (left > 0 && right < 0) {
            // left positive, right negative
            return switch (operationType) {
                case GT, GTEQ:
                    yield new Pair<>(null, null);
                case LT:
                    yield new Pair<>(left / right + 1L, -1L);
                case LTEQ:
                    yield new Pair<>(left / right, -1L);
                default:
                    throw new AerospikeDSLException("OperationType not supported for division: " + operationType);
            };
        } else if (right > 0 && left < 0) {
            // right positive, left negative
            return switch (operationType) {
                case GT:
                    yield new Pair<>(left / right + 1, -1L);
                case GTEQ:
                    yield new Pair<>(left / right, -1L);
                case LT, LTEQ:
                    yield new Pair<>(null, null);
                default:
                    throw new AerospikeDSLException("OperationType not supported for division: " + operationType);
            };
        } else if ((left > 0 && right == 0) || (left < 0 && right == 0)) {
            throw new AerospikeDSLException("Division by zero is not allowed");
        } else if ((left == 0 && right > 0) || ((left == 0 && right < 0))) {
            return new Pair<>(null, null);
        }
        return new Pair<>(null, null);
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
                ValidationUtils.validateComparableTypes(bin.getExpType(), Exp.Type.INT);
                yield getFilterForArithmeticOrFail(binName, ((IntOperand) operand).getValue(), type);

            }
            case STRING_OPERAND -> {
                if (type != EQ) throw new AerospikeDSLException("Operand type not supported");

                if (bin.getExpType() != null &&
                        bin.getExpType().equals(Exp.Type.BLOB)) {
                    // Base64 Blob
                    ValidationUtils.validateComparableTypes(bin.getExpType(), Exp.Type.BLOB);
                    String base64String = ((StringOperand) operand).getValue();
                    byte[] value = Base64.getDecoder().decode(base64String);
                    yield Filter.equal(binName, value);
                } else {
                    // String
                    ValidationUtils.validateComparableTypes(bin.getExpType(), Exp.Type.STRING);
                    yield Filter.equal(binName, ((StringOperand) operand).getValue());
                }
            }
            default -> throw new AerospikeDSLException("Operand type not supported: %s".formatted(operand.getPartType()));
        };
    }

    private static Filter applyFilterOperator(String binName, IntOperand leftOperand, IntOperand rightOperand,
                                              Expr.ExprPartsOperation operationType, FilterOperationType type,
                                              ArithmeticTermType termType) {
        long leftValue = leftOperand.getValue();
        long rightValue = rightOperand.getValue();
        long value;
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
            Pair<Long, Long> valueForDiv = getLimitsForDivision(leftValue, rightValue, type, termType);
            if (valueForDiv.a == null
                    || valueForDiv.b == null
                    || valueForDiv.a > valueForDiv.b
                    || (valueForDiv.a == 0 && valueForDiv.b == 0)) {
                throw new AerospikeDSLException("The operation is not supported by secondary index filter");
            }
            return getFilterForDivOrFail(binName, valueForDiv, type);
        } else if (operationType == MUL) {
            if (leftValue <= 0) {
                if (leftValue == 0) throw new AerospikeDSLException("Cannot divide by zero");
                type = invertType(type);
            }
            value = rightValue / leftValue;
        } else {
            throw new UnsupportedOperationException("Not supported");
        }

        return getFilterForArithmeticOrFail(binName, value, type);
    }

    private static Filter getFilterForArithmeticOrFail(String binName, long value, FilterOperationType type) {
        return switch (type) {
            // "$.intBin1 > 100" and "100 < $.intBin1" represent the same Filter
            case GT -> Filter.range(binName, value + 1, Long.MAX_VALUE);
            case GTEQ -> Filter.range(binName, value, Long.MAX_VALUE);
            case LT -> Filter.range(binName, Long.MIN_VALUE, value - 1);
            case LTEQ -> Filter.range(binName, Long.MIN_VALUE, value);
            case EQ -> Filter.equal(binName, value);
            default -> throw new AerospikeDSLException("The operation is not supported by secondary index filter");
        };
    }

    private FilterOperationType invertType(FilterOperationType type) {
        return switch (type) {
            case GT -> LT;
            case GTEQ -> LTEQ;
            case LT -> GT;
            case LTEQ -> GTEQ;
            default -> type;
        };
    }
}
