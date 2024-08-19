package com.aerospike.dsl.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MapRankRange {
    private boolean inverted;
    private Integer start;
    private Integer count;
}
