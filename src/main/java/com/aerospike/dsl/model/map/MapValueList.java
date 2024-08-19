package com.aerospike.dsl.model.map;


import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import lombok.Getter;

import java.util.List;

import static com.aerospike.dsl.util.ParsingUtils.getWithoutQuotes;

@Getter
public class MapValueList extends MapPart {
    private final boolean inverted;
    private final List<?> valueList;

    public MapValueList(boolean inverted, List<?> valueList) {
        super(MapPartType.VALUE_LIST);
        this.inverted = inverted;
        this.valueList = valueList;
    }

    public static MapValueList constructFromCTX(ConditionParser.MapValueListContext ctx) {
        ConditionParser.StandardMapValueListContext valueList = ctx.standardMapValueList();
        ConditionParser.InvertedMapValueListContext invertedValueList = ctx.invertedMapValueList();

        if (valueList != null || invertedValueList != null) {
            ConditionParser.ValueListIdentifierContext list =
                    valueList != null ? valueList.valueListIdentifier() : invertedValueList.valueListIdentifier();
            boolean isInverted = valueList == null;

            List<?> valueListObjects = list.valueIdentifier().stream().map(
                    listValue -> {
                        if (listValue.NAME_IDENTIFIER() != null) {
                            return listValue.NAME_IDENTIFIER().getText();
                        } else if (listValue.QUOTED_STRING() != null) {
                            return getWithoutQuotes(listValue.QUOTED_STRING().getText());
                        } else {
                            return Integer.parseInt(listValue.INT().getText());
                        }
                    }
            ).toList();

            return new MapValueList(isInverted, valueListObjects);
        }
        throw new AerospikeDSLException("Could not translate MapValueList from ctx: %s".formatted(ctx));
    }
}
