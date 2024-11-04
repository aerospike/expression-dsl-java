package com.aerospike.dsl.model;

import com.aerospike.client.exp.Exp;
import lombok.Getter;

@Getter
public class MetadataOperand extends Expr {

    private final String functionName;

    public MetadataOperand(String functionName) {
        super(constructExp(functionName, null));
        this.partType = PartType.METADATA_OPERAND;
        this.functionName = functionName;
    }

    public MetadataOperand(String functionName, int parameter) {
        super(constructExp(functionName, parameter));
        this.partType = PartType.METADATA_OPERAND;
        this.functionName = functionName;
    }

    private static Exp constructExp(String functionName, Integer parameter) {
        return switch (functionName) {
            case "deviceSize" -> Exp.deviceSize();
            case "memorySize" -> Exp.memorySize();
            case "recordSize" -> Exp.recordSize();
            case "digestModulo" -> Exp.digestModulo(parameter);
            case "isTombstone" -> Exp.isTombstone();
            case "keyExists" -> Exp.keyExists();
            case "lastUpdate" -> Exp.lastUpdate();
            case "sinceUpdate" -> Exp.sinceUpdate();
            case "setName" -> Exp.setName();
            case "ttl" -> Exp.ttl();
            case "voidTime" -> Exp.voidTime();
            default -> throw new IllegalArgumentException("Unknown metadata function: " + functionName);
        };
    }

    public MetadataReturnType getMetadataType() {
        return switch (functionName) {
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
            default -> throw new IllegalArgumentException("Unknown metadata function: " + functionName);
        };
    }

    public enum MetadataReturnType {
        INT,
        STRING,
        BOOL,
    }
}
