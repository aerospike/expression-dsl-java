package com.aerospike.dsl.part.operand;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.part.AbstractPart;
import lombok.Getter;

import java.util.List;

@Getter
public class ListOperand extends AbstractPart implements ParsedValueOperand {

    private final List<Object> value;

    public ListOperand(List<Object> list) {
        super(PartType.LIST_OPERAND);
        this.value = list;
    }

    public Exp getExp() {
        return Exp.val(value);
    }
}
