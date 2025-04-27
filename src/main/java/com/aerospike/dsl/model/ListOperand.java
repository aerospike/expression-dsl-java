package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

import java.util.List;

@Getter
public class ListOperand extends AbstractPart implements ParsedOperand {

    private final List<Object> value;

    public ListOperand(List<Object> list) {
        super(PartType.LIST_OPERAND);
        this.value = list;
    }

    public Exp getExp() {
        if (exp == null) exp = Exp.val(value);
        return exp;
    }
}
