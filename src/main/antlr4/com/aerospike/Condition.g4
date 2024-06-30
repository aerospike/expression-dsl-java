grammar Condition;

@header {
    package com.aerospike;
}

parse   : expression;

expression
    : expression 'and' expression    # AndExpression
    | expression 'or' expression     # OrExpression
    | operand '>' operand            # GreaterThanExpression
    | operand '<' operand            # LessThanExpression
    | operand '==' operand           # EqualityExpression
    | operand                        # OperandExpression
    ;

operand: binName | functionCall | NUMBER | STRING | quotedString;

binName : '$.' BIN_NAME_IDENTIFIER;

BIN_NAME_IDENTIFIER : [a-zA-Z0-9_]*;

functionCall
    : '$.' functionName '()';

functionName: 'deviceSize' | 'ttl';

NUMBER  : [0-9]+;

STRING  : [a-zA-Z_][a-zA-Z0-9_]*;

quotedString : ('\'' (~'\'')* '\'') | ('"' (~'"')* '"');

WS  : [ \t\r\n]+ -> skip;