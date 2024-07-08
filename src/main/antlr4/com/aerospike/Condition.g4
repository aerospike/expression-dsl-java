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

METADATA_FUNCTION
    : 'deviceSize' { _input.LA(1) == '(' }?
    | 'memorySize' { _input.LA(1) == '(' }?
    | 'recordSize' { _input.LA(1) == '(' }?
    | 'isTombstone' { _input.LA(1) == '(' }?
    | 'keyExists' { _input.LA(1) == '(' }?
    | 'lastUpdate' { _input.LA(1) == '(' }?
    | 'sinceUpdate' { _input.LA(1) == '(' }?
    | 'setName' { _input.LA(1) == '(' }?
    | 'ttl' { _input.LA(1) == '(' }?
    | 'voidTime' { _input.LA(1) == '(' }?
    ;

pathPart: NAME_IDENTIFIER;

NAME_IDENTIFIER : [a-zA-Z0-9_]+;

metadata: metadataFunction '(' ')' | digestModulo '(' NUMBER ')';

metadataFunction: METADATA_FUNCTION;

pathFunction: 'exists' '( )';

digestModulo: 'digestModulo' { _input.LA(1) == '(' }?;

WS: [ \t\r\n]+ -> skip;