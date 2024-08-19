package com.aerospike.dsl.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ListIndexRange {
    private boolean inverted;
    private Integer start;
    private Integer count;
}
