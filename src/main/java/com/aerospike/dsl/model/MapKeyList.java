package com.aerospike.dsl.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class MapKeyList {
    private boolean inverted;
    private List<String> keyList;
}