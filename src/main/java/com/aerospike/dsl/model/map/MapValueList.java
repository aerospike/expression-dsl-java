package com.aerospike.dsl.model.map;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class MapValueList {
    private boolean inverted;
    private List<?> valueList;
}
