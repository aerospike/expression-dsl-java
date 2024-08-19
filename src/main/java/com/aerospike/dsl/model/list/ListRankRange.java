package com.aerospike.dsl.model.list;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ListRankRange {
    private boolean inverted;
    private Integer start;
    private Integer count;
}
