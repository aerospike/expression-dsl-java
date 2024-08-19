package com.aerospike.dsl.model.cdt.map;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.BasePath;
import lombok.Getter;

@Getter
public class MapKeyBin extends MapPart {
    private final String key;

    public MapKeyBin(String key) {
        super(MapPart.MapPartType.BIN);
        this.key = key;
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return Exp.mapBin(basePath.getBinPart().getBinName());
    }
}
