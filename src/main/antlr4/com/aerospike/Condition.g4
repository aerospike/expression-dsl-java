grammar Condition;

@header {
    package com.aerospike;
}

parse   : expression;

expression
    : expression 'and' expression    # AndExpression
    | expression 'or' expression     # OrExpression
    | operand '>' operand            # GreaterThanExpression
    | operand '>=' operand           # GreaterThanOrEqualExpression
    | operand '<' operand            # LessThanExpression
    | operand '<=' operand           # LessThanOrEqualExpression
    | operand '==' operand           # EqualityExpression
    | operand '!=' operand           # InequalityExpression
    | operand                        # OperandExpression
    ;

operand: number | quotedString | path | metadata | '(' expression ')';

number : NUMBER;

NUMBER  : '-'?[0-9]+;

quotedString : ('\'' (~'\'')* '\'') | ('"' (~'"')* '"');

path : '$.' NAME_IDENTIFIER ('.' NAME_IDENTIFIER)*? ('.' pathFunction)?;

NAME_IDENTIFIER : [a-zA-Z0-9_]+;

metadata
    : '$.' metadataFunction '(' ')'
    | '$.' digestModulo '(' NUMBER ')'
    ;

metadataFunction
    : 'deviceSize'
    | 'memorySize'
    | 'recordSize'
    | 'isTombstone'
    | 'keyExists'
    | 'lastUpdate'
    | 'sinceUpdate'
    | 'setName'
    | 'ttl'
    | 'voidTime'
    ;

pathFunction: 'exists';

digestModulo: 'digestModulo';

WS  : [ \t\r\n]+ -> skip;