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

operand: NUMBER | quotedString | path | functionCall | '(' expression ')';

NUMBER  : [0-9]+;

quotedString : ('\'' (~'\'')* '\'') | ('"' (~'"')* '"');

path : '$.' NAME_IDENTIFIER ('.' NAME_IDENTIFIER)*? ('.' functionName)?;

NAME_IDENTIFIER : [a-zA-Z0-9_]+;

functionCall : '$.' functionName;

functionName : ('deviceSize' | 'ttl' | 'exists')  '()';

WS  : [ \t\r\n]+ -> skip;