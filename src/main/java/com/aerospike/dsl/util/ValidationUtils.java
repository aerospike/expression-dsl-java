package com.aerospike.dsl.util;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.DslParseException;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ValidationUtils {

    public static void validateComparableTypes(Exp.Type leftType, Exp.Type rightType) {
        if (leftType != null && rightType != null) {
            boolean isIntAndFloat =
                    (leftType.equals(Exp.Type.INT) && rightType.equals(Exp.Type.FLOAT)) ||
                            (leftType.equals(Exp.Type.FLOAT) && rightType.equals(Exp.Type.INT));

            if (!leftType.equals(rightType) && !isIntAndFloat) {
                throw new DslParseException("Cannot compare %s to %s".formatted(leftType, rightType));
            }
        }
    }
}
