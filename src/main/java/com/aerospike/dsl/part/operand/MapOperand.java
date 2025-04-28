package com.aerospike.dsl.part.operand;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.part.AbstractPart;
import lombok.Getter;

import java.util.TreeMap;

@Getter
public class MapOperand extends AbstractPart implements ParsedValueOperand {

    private final TreeMap<Object, Object> value;

    public MapOperand(TreeMap<Object, Object> map) {
        super(PartType.MAP_OPERAND);
        this.value = map;
    }

    public Exp getExp() {
        return Exp.val(value);
    }
}
