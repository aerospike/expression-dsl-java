package com.aerospike.dsl.model.cdt.list;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;
import com.aerospike.dsl.model.BasePath;

public class ListBin extends ListPart {

    public ListBin() {
        super(ListPartType.LIST_BIN);
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return Exp.listBin(basePath.getBinPart().getBinName());
    }
}
