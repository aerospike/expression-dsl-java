package com.aerospike.dsl.parts.cdt.list;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.cdt.ListReturnType;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.ListExp;
import com.aerospike.dsl.parts.path.BasePath;

import java.util.List;

import static com.aerospike.dsl.util.ParsingUtils.parseSignedInt;
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
                        return parseSignedInt(listValue.signedInt());
                    }
            ).toList();

            return new ListValueList(isInverted, valueListObjects);
        }
        throw new DslParseException("Could not translate ListValueList from ctx: %s".formatted(ctx));
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
