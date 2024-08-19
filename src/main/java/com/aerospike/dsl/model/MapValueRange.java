package com.aerospike.dsl.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MapValueRange {
    private boolean inverted;
    private Integer start;
    private Integer end;
}
