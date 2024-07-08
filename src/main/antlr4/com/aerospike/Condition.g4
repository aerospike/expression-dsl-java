grammar Condition;

@header {
    package com.aerospike;
}

parse: expression;

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

operand: number | quotedString | '$.' pathOrMetadata | '(' expression ')';

number: NUMBER;

NUMBER: '-'?[0-9]+;

quotedString: ('\'' (~'\'')* '\'') | ('"' (~'"')* '"');

pathOrMetadata: path | metadata;

path: pathPart ('.' pathPart)*? ('.' pathFunction)?;

metadata: metadataFunction | DIGEST_MODULO '(' NUMBER ')';

metadataFunction: METADATA_FUNCTION;

METADATA_FUNCTION
    : 'deviceSize()'
    | 'memorySize()'
    | 'recordSize()'
    | 'isTombstone()'
    | 'keyExists()'
    | 'lastUpdate()'
    | 'sinceUpdate()'
    | 'setName()'
    | 'ttl()'
    | 'voidTime()'
    ;

DIGEST_MODULO: 'digestModulo' { _input.LA(1) == '(' }?;

pathPart: NAME_IDENTIFIER;

NAME_IDENTIFIER : [a-zA-Z0-9_]+;

pathFunction: 'exists' '( )';

WS: [ \t\r\n]+ -> skip;