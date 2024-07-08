grammar Condition;

@header {
    package com.aerospike;
}

parse   : expression;

expression
    : expression 'and' expression                  # AndExpression
    | expression 'or' expression                   # OrExpression
    | 'not' '('expression')'                       # NotExpression
    | 'exclusive' '('expression',' expression')'   # ExclusiveExpression
    | operand '>' operand                          # GreaterThanExpression
    | operand '>=' operand                         # GreaterThanOrEqualExpression
    | operand '<' operand                          # LessThanExpression
    | operand '<=' operand                         # LessThanOrEqualExpression
    | operand '==' operand                         # EqualityExpression
    | operand '!=' operand                         # InequalityExpression
    | operand                                      # OperandExpression
    ;

operand: number | quotedString | path | metadata | '(' expression ')';

number : NUMBER;

NUMBER  : '-'?[0-9]+;

quotedString : ('\'' (~'\'')* '\'') | ('"' (~'"')* '"');

path : '$.' pathPart ('.' pathPart)*? ('.' pathFunction)?;

METADATA_FUNCTION
    : 'deviceSize'
    | 'memorySize'
    | 'recordSize'
    | 'isTombstone'
    | 'keyExists'
    | 'lastUpdate'
    | 'sinceUpdate' { _input.LA(1) == '(' }?
    | 'setName'
    | 'ttl'
    | 'voidTime'
    ;

pathPart : NAME_IDENTIFIER;

NAME_IDENTIFIER : [a-zA-Z0-9_]+;

metadata
    : '$.' metadataFunction '(' ')'
    | '$.' digestModulo '(' NUMBER ')'
    ;

metadataFunction
    : METADATA_FUNCTION;

pathFunction: 'exists' '( )';

digestModulo: 'digestModulo';

WS  : [ \t\r\n]+ -> skip;