package com.aerospike.dsl.parts.operand;

import com.aerospike.dsl.DslParseException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OperandFactoryTests {

    @Test
    void negMapWithIncomparableKeys() {
        Map<Object, Object> mixedKeyMap = new HashMap<>();
        mixedKeyMap.put(1, "a");
        mixedKeyMap.put("b", 2);
        assertThatThrownBy(() -> OperandFactory.createOperand(mixedKeyMap))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("mutually comparable");
    }

    @Test
    void negMapWithNullKey() {
        Map<Object, Object> nullKeyMap = new HashMap<>();
        nullKeyMap.put(null, "value");
        assertThatThrownBy(() -> OperandFactory.createOperand(nullKeyMap))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("mutually comparable");
    }
}
