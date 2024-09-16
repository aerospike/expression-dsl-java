package com.aerospike.dsl.model.cdt.map;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.BasePath;
import com.aerospike.dsl.model.cdt.CdtPart;
import lombok.Getter;

/**
 * Designates that the element to the left is a Map.
 */
@Getter
public class MapTypeDesignator extends MapPart {

    public MapTypeDesignator() {
        super(MapPartType.MAP_TYPE_DESIGNATOR);
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        var partsUpToDesignator = basePath.getParts().subList(0, basePath.getParts().size() - 1);
        BasePath basePathUntilDesignator = new BasePath(basePath.getBinPart(), partsUpToDesignator);
        int partsUpToDesignatorSize = partsUpToDesignator.size();
        if ((partsUpToDesignatorSize > 0)) {
            return ((CdtPart) partsUpToDesignator.get(partsUpToDesignatorSize - 1))
                    .constructExp(basePathUntilDesignator, valueType, cdtReturnType, context);
        } else {
            // only bin
            return Exp.mapBin(basePath.getBinPart().getBinName());
        }
    }

    public static MapTypeDesignator constructFromCTX() {
        return new MapTypeDesignator();
    }
}
