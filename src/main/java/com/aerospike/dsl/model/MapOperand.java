package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

import java.util.TreeMap;

@Getter
public class MapOperand extends AbstractPart implements ParsedOperand {

    private final TreeMap<Object, Object> value;

    public MapOperand(TreeMap<Object, Object> map) {
        super(PartType.MAP_OPERAND, Exp.val(map));
        this.value = map;
    }
}
