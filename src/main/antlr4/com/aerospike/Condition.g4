grammar Condition;

@header {
    package com.aerospike;
}

parse: expression;

expression
    // Logical Expressions
    : expression 'and' expression                  # AndExpression
    | expression 'or' expression                   # OrExpression
    | 'not' '('expression')'                       # NotExpression
    | 'exclusive' '('expression',' expression')'   # ExclusiveExpression
    // Comparison Expressions
    | operand '>' operand                          # GreaterThanExpression
    | operand '>=' operand                         # GreaterThanOrEqualExpression
    | operand '<' operand                          # LessThanExpression
    | operand '<=' operand                         # LessThanOrEqualExpression
    | operand '==' operand                         # EqualityExpression
    | operand '!=' operand                         # InequalityExpression
    // Arithmetic Expressions
    | operand '+' operand                          # AddExpression
    | operand '-' operand                          # SubExpression
    | operand '*' operand                          # MulExpression
    | operand '/' operand                          # DivExpression
    | operand '%' operand                          # ModExpression
    | operand '&' operand                          # IntAndExpression
    | operand '|' operand                          # IntOrExpression
    | operand '^' operand                          # IntXorExpression
    | '~' operand                                  # IntNotExpression
    | operand '<<' operand                         # IntLShiftExpression
    | operand '>>' operand                         # IntRShiftExpression
    // Base Operand
    | operand                                      # OperandExpression
    ;

operand
    : numberOperand
    | quotedString
    | '$.' pathOrMetadata
    | '(' expression ')'
    ;

numberOperand: intOperand | floatOperand;

intOperand: INT;
floatOperand: FLOAT;

INT: '-'?[0-9]+;
FLOAT: '-'? [0-9]+ '.' [0-9]+;

quotedString: QUOTED_STRING;

QUOTED_STRING: ('\'' (~'\'')* '\'') | ('"' (~'"')* '"');

pathOrMetadata: path | metadata;

path: pathPart ('.' pathPart)*? ('.' pathFunction)?;

metadata: METADATA_FUNCTION;

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
    | 'digestModulo(' INT ')'
    ;

pathPart: NAME_IDENTIFIER;

NAME_IDENTIFIER : [a-zA-Z0-9_]+;

pathFunction: 'exists' '( )';

WS: [ \t\r\n]+ -> skip;