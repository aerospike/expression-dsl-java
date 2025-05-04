package com.aerospike.dsl.util;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.cdt.map.MapPart;
import com.aerospike.dsl.parts.cdt.map.MapTypeDesignator;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TypeUtils {

    public static Exp.Type getDefaultType(AbstractPart part) {
        if (part instanceof MapPart
                // MapTypeDesignator is usually combined with int based operations such as size
                && !(part instanceof MapTypeDesignator)) {
            // For all other Map parts the default type should be STRING
            return Exp.Type.STRING;
        } else {
            // Default INT
            return Exp.Type.INT;
        }
    }

    // When return type is COUNT, always return INT as default type
    public static Exp.Type getDefaultTypeForCount() {
        return Exp.Type.INT;
    }
}
