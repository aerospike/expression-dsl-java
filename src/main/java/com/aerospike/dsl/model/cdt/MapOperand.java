package com.aerospike.dsl.model.cdt;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.model.ParsedOperand;
import lombok.Getter;

import java.util.TreeMap;

@Getter
public class MapOperand extends AbstractPart implements ParsedOperand {

    private final TreeMap<Object, Object> value;

    public MapOperand(TreeMap<Object, Object> map) {
        super(PartType.MAP_OPERAND);
        this.value = map;
    }

    public Exp getExp() {
        return Exp.val(value);
    }
}
