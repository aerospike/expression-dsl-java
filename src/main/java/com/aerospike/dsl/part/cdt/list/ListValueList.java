package com.aerospike.dsl.part.cdt.list;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.exception.AerospikeDSLException;
import com.aerospike.dsl.part.path.BasePath;

import java.util.List;

import static com.aerospike.dsl.util.ParsingUtils.unquote;

public class ListValueList extends ListPart {
    private final boolean inverted;
    private final List<?> valueList;

    public ListValueList(boolean inverted, List<?> valueList) {
        super(ListPartType.VALUE_LIST);
        this.inverted = inverted;
        this.valueList = valueList;
    }

    public static ListValueList from(ConditionParser.ListValueListContext ctx) {
        ConditionParser.StandardListValueListContext valueList = ctx.standardListValueList();
        ConditionParser.InvertedListValueListContext invertedValueList = ctx.invertedListValueList();

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

            return new ListValueList(isInverted, valueListObjects);
        }
        throw new AerospikeDSLException("Could not translate ListValueList from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (inverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        return ListExp.getByValueList(cdtReturnType, Exp.val(valueList),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }
}
