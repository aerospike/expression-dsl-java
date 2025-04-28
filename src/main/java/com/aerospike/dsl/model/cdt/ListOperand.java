package com.aerospike.dsl.model.cdt;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.model.ParsedOperand;
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
        return Exp.val(value);
    }
}
