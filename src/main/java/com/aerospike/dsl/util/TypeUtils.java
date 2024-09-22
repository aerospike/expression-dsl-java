package com.aerospike.dsl.util;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.model.cdt.map.MapKey;
import com.aerospike.dsl.model.cdt.map.MapKeyList;
import com.aerospike.dsl.model.cdt.map.MapKeyRange;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TypeUtils {

    public static Exp.Type getDefaultType(AbstractPart part) {
        if (part instanceof MapKey ||
                part instanceof MapKeyList ||
                part instanceof MapKeyRange) {
            // Map default type should be STRING
            return Exp.Type.STRING;
        } else {
            // Default INT
            return Exp.Type.INT;
        }
    }
}
