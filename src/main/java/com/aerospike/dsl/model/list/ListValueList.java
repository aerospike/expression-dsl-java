package com.aerospike.dsl.model.list;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class ListValueList {
    private boolean inverted;
    private List<?> valueList;
}
