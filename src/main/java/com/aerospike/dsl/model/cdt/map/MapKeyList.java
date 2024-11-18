package com.aerospike.dsl.model.cdt.map;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.BasePath;

import java.util.List;

import static com.aerospike.dsl.util.ParsingUtils.getWithoutQuotes;

public class MapKeyList extends MapPart {
    private final boolean inverted;
    private final List<String> keyList;

    public MapKeyList(boolean inverted, List<String> keyList) {
        super(MapPartType.KEY_LIST);
        this.inverted = inverted;
        this.keyList = keyList;
    }

    public static MapKeyList from(ConditionParser.MapKeyListContext ctx) {
        ConditionParser.StandardMapKeyListContext keyList = ctx.standardMapKeyList();
        ConditionParser.InvertedMapKeyListContext invertedKeyList = ctx.invertedMapKeyList();

        if (keyList != null || invertedKeyList != null) {
            ConditionParser.KeyListIdentifierContext list =
                    keyList != null ? keyList.keyListIdentifier() : invertedKeyList.keyListIdentifier();
            boolean isInverted = keyList == null;

            List<String> keyListStrings = list.mapKey().stream().map(
                    mapKey -> {
                        if (mapKey.NAME_IDENTIFIER() != null) {
                            return mapKey.NAME_IDENTIFIER().getText();
                        } else {
                            return getWithoutQuotes(mapKey.QUOTED_STRING().getText());
                        }
                    }
            ).toList();

            return new MapKeyList(isInverted, keyListStrings);
        }
        throw new AerospikeDSLException("Could not translate MapKeyList from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (inverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        return MapExp.getByKeyList(cdtReturnType, Exp.val(keyList),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }
}