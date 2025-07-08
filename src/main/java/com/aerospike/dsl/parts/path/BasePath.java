package com.aerospike.dsl.parts.path;

import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;

import java.util.List;

@Getter
public class BasePath extends AbstractPart {

    private final BinPart binPart;
    private final List<AbstractPart> cdtParts;

    public BasePath(BinPart binPart, List<AbstractPart> cdtParts) {
        super(PartType.BASE_PATH);
        this.binPart = binPart;
        this.cdtParts = cdtParts;
    }

    // Bin type is determined by the base path's first element
    public Exp.Type getBinType() {
        if (!cdtParts.isEmpty()) {
            return switch (cdtParts.get(0).getPartType()) {
                case MAP_PART -> Exp.Type.MAP;
                case LIST_PART -> Exp.Type.LIST;
                default -> null;
            };
        }
        return binPart.getExpType();
    }
}
