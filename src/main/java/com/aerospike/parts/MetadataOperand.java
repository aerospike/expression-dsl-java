package com.aerospike.parts;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class MetadataOperand extends AbstractPart {

    private final String metadataFunction;

    public MetadataOperand(Exp exp, String metadataFunction) {
        super(Type.METADATA_OPERAND, exp);
        this.metadataFunction = metadataFunction;
    }

    public MetadataReturnType getMetadataType() {
        return switch (metadataFunction) {
            case "deviceSize",
                 "memorySize",
                 "recordSize",
                 "digestModulo",
                 "lastUpdate",
                 "sinceUpdate",
                 "ttl",
                 "voidTime" -> MetadataReturnType.INT;
            case "isTombstone",
                 "keyExists" -> MetadataReturnType.BOOL;
            case "setName" -> MetadataReturnType.STRING;
            default -> throw new IllegalArgumentException("Unknown metadata function: " + metadataFunction);
        };
    }

    public enum MetadataReturnType {
        INT,
        STRING,
        BOOL,
    }
}
