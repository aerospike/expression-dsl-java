package com.aerospike.dsl.model.map;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.util.ParsingUtils;
import lombok.Getter;

import java.util.List;

@Getter
public class MapKeyList extends MapPart {
    private final boolean inverted;
    private final List<String> keyList;

    public MapKeyList(boolean inverted, List<String> keyList) {
        super(MapPartType.KEY_LIST);
        this.inverted = inverted;
        this.keyList = keyList;
    }

    public static MapKeyList constructFromCTX(ConditionParser.MapKeyListContext ctx) {
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
                            return ParsingUtils.getWithoutQuotes(mapKey.QUOTED_STRING().getText());
                        }
                    }
            ).toList();

            return new MapKeyList(isInverted, keyListStrings);
        }
        throw new AerospikeDSLException("Could not translate MapKeyList from ctx: %s".formatted(ctx));
    }
}