package com.aerospike.dsl.model.cdt.list;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.AbstractPart;
import com.aerospike.dsl.model.BasePath;
import com.aerospike.dsl.model.cdt.CdtPart;

import java.util.ArrayList;
import java.util.List;

/**
 * Designates that element to the left is a List.
 */
public class ListTypeDesignator extends ListPart {

    public ListTypeDesignator() {
        super(ListPartType.LIST_TYPE_DESIGNATOR);
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        List<AbstractPart> partsUpToDesignator = !basePath.getParts().isEmpty()
                ? basePath.getParts().subList(0, basePath.getParts().size() - 1)
                : new ArrayList<>();
        BasePath basePathUntilDesignator = new BasePath(basePath.getBinPart(), partsUpToDesignator);
        if ((!partsUpToDesignator.isEmpty())) {
            return ((CdtPart) partsUpToDesignator.get(partsUpToDesignator.size() - 1))
                    .constructExp(basePathUntilDesignator, valueType, cdtReturnType, context);
        } else {
            // only bin
            return Exp.listBin(basePath.getBinPart().getBinName());
        }
    }

    public static ListTypeDesignator constructFromCTX() {
        return new ListTypeDesignator();
    }
}
