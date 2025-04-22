package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;
import lombok.Setter;

import java.util.Base64;

@Getter
public class StringOperand extends AbstractPart implements ParsedOperand {

    private final String value;
    @Setter
    private boolean isBlob = false;

    public StringOperand(String string) {
        super(PartType.STRING_OPERAND);
        this.value = string;
    }

    public Exp getExp() {
        if (isBlob) {
            byte[] byteValue = Base64.getDecoder().decode(value);
            return Exp.val(byteValue);
        }
        return Exp.val(value);
    }
}
