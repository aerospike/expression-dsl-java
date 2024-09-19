package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

import java.util.List;

@Getter
public class BasePath extends AbstractPart {

    private final BinPart binPart;
    private final List<AbstractPart> parts;

    public BasePath(BinPart binOperand, List<AbstractPart> parts) {
        super(PartType.BASE_PATH);
        this.binPart = binOperand;
        this.parts = parts;
    }

    // Bin type is determined by the base path's first element
    public Exp.Type getBinType() {
        if (!this.getParts().isEmpty()) {
            return switch (this.getParts().get(0).getPartType()) {
                case MAP_PART -> Exp.Type.MAP;
                case LIST_PART -> Exp.Type.LIST;
                default -> null;
            };
        } else {
            return this.getBinPart().getExpType();
        }
    }
}
