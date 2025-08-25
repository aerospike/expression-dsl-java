package com.aerospike.dsl.parts.operand;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;

import java.util.List;

@Getter
public class ListOperand extends AbstractPart implements ParsedValueOperand {

    private final List<Object> value;

    public ListOperand(List<Object> list) {
        super(PartType.LIST_OPERAND);
        this.value = list;
    }

    @Override
    public PartType getType() {
        return PartType.LIST_OPERAND;
    }

    @Override
    public Exp getExp() {
        return Exp.val(value);
    }
}
