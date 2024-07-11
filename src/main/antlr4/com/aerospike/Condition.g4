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

quotedString: QUOTED_STRING;

QUOTED_STRING: ('\'' (~'\'')* '\'') | ('"' (~'"')* '"');

pathOrMetadata: path | metadata;

path: basePath ('.' pathFunction)?;

basePath: pathPart ('.' (pathPart | listPath))*?;

metadata: METADATA_FUNCTION | digestModulo;

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

digestModulo: DIGEST_MODULO '(' NUMBER ')';

DIGEST_MODULO: 'digestModulo' { _input.LA(1) == '(' }?; // next character is a '('

PATH_FUNCTION_GET: 'get';

pathFunctionParamName
    : PATH_FUNCTION_PARAM_TYPE
    | PATH_FUNCTION_PARAM_RETURN
    ;
    
PATH_FUNCTION_PARAM_TYPE: 'type';

PATH_FUNCTION_PARAM_RETURN: 'return';

pathFunctionParamValue: PATH_FUNCTION_PARAM_TYPE_VALUE | PATH_FUNCTION_PARAM_RETURN_VALUE;

PATH_FUNCTION_PARAM_TYPE_VALUE
    : 'INT'
    | 'STR'
    | 'HLL'
    | 'BLOB'
    | 'FLOAT'
    | 'BOOL'
    | 'LIST'
    | 'MAP'
    | 'GEO'
    ;

PATH_FUNCTION_PARAM_RETURN_VALUE
    : 'VALUE'
    | 'COUNT'
    | 'NONE'
    ;

pathPart: NAME_IDENTIFIER;

listPath: LIST_BIN | listIndex | listValue | listRank;

LIST_BIN: '[]';

listIndex: '[' NUMBER ']';

listValue: '[' '=' NAME_IDENTIFIER ']';

listRank: '[' '#' NUMBER ']';

pathFunction
    : pathFunctionExists
    | pathFunctionGet
    | PATH_FUNCTION_COUNT
    | 'remove' '()'
    | 'insert' '()'
    | 'set' '()'
    | 'append' '()'
    | 'increment' '()'
    | 'clear' '()'
    | 'sort' '()'
    | pathFunctionSize
    ;

pathFunctionExists: PATH_FUNCTION_EXISTS;

PATH_FUNCTION_EXISTS: 'exists' '()';

pathFunctionCount: PATH_FUNCTION_COUNT;

PATH_FUNCTION_COUNT: 'count' '()';

pathFunctionSize: PATH_FUNCTION_SIZE;

PATH_FUNCTION_SIZE: 'size' '()';

pathFunctionGet
    : PATH_FUNCTION_GET '(' pathFunctionParams ')'
    | PATH_FUNCTION_GET '()'
    ;

pathFunctionParams: pathFunctionParam (',' pathFunctionParam)*?;

pathFunctionParam: pathFunctionParamName ':' pathFunctionParamValue;

NAME_IDENTIFIER: [a-zA-Z0-9_]+;

WS: [ \t\r\n]+ -> skip;