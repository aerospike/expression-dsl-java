package com.aerospike.dsl.part.path;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.part.AbstractPart;
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
        if (!parts.isEmpty()) {
            return switch (parts.get(0).getPartType()) {
                case MAP_PART -> Exp.Type.MAP;
                case LIST_PART -> Exp.Type.LIST;
                default -> null;
            };
        }
        return binPart.getExpType();
    }
}
