grammar Condition;

@header {
    package com.aerospike;
}

parse   : expression EOF ;

expression
    : expression 'and' expression    # AndExpression
    | expression 'or' expression     # OrExpression
    | operand '>' operand            # GreaterThanExpression
    | operand '<' operand            # LessThanExpression
    | operand                        # OperandExpression
    ;

operand
    : functionCall
    | NUMBER
    ;

functionCall
    : '$.' functionName '()'
    ;

functionName
    : 'deviceSize'
    | 'ttl'
    ;

NUMBER  : [0-9]+ ;

WS  : [ \t\r\n]+ -> skip ;