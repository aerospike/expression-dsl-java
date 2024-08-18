package com.aerospike.dsl.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MapKeyRange {
    private boolean inverted;
    private String start;
    private String end;
}