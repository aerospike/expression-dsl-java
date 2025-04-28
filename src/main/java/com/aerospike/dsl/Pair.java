package com.aerospike.dsl;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Pair<A, B> {

    A filter;
    B exp;
}
