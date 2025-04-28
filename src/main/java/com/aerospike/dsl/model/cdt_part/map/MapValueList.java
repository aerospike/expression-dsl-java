package com.aerospike.dsl.model.cdt_part.map;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.model.path.BasePath;

import java.util.List;

import static com.aerospike.dsl.util.ParsingUtils.unquote;

public class MapValueList extends MapPart {
    private final boolean inverted;
    private final List<?> valueList;

    public MapValueList(boolean inverted, List<?> valueList) {
        super(MapPartType.VALUE_LIST);
        this.inverted = inverted;
        this.valueList = valueList;
    }

    public static MapValueList from(ConditionParser.MapValueListContext ctx) {
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
                            return unquote(listValue.QUOTED_STRING().getText());
                        }
                        return Integer.parseInt(listValue.INT().getText());
                    }
            ).toList();

            return new MapValueList(isInverted, valueListObjects);
        }
        throw new AerospikeDSLException("Could not translate MapValueList from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (inverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        return MapExp.getByValueList(cdtReturnType, Exp.val(valueList),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }
}
