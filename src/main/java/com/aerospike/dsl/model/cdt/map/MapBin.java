package com.aerospike.dsl.model.cdt.map;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.BasePath;
import lombok.Getter;

/**
 * Acts as a synthetic Map bin part, used for map.size() scenarios when we don't have any parts.
 * This is not needed in lists because list have [] for list bins.
 */
@Getter
public class MapBin extends MapPart {

    public MapBin() {
        super(MapPartType.BIN);
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return Exp.mapBin(basePath.getBinPart().getBinName());
    }
}
