grammar Condition;

@header {
    package com.aerospike.dsl;
}

parse: expression;

expression
    // Declaration and Control Expressions
    : 'with' '(' variableDefinition (',' variableDefinition)* ')' 'do' '(' expression ')'       # WithExpression
    | 'when' '(' expressionMapping (',' expressionMapping)* ',' 'default' '=>' expression ')'   # WhenExpression
    // Logical Expressions
    | expression 'and' expression                                                               # AndExpression
    | expression 'or' expression                                                                # OrExpression
    | 'not' '(' expression ')'                                                                  # NotExpression
    | 'exclusive' '(' expression (',' expression)+ ')'                                          # ExclusiveExpression
    // Comparison Expressions
    | operand '>' operand                                                                       # GreaterThanExpression
    | operand '>=' operand                                                                      # GreaterThanOrEqualExpression
    | operand '<' operand                                                                       # LessThanExpression
    | operand '<=' operand                                                                      # LessThanOrEqualExpression
    | operand '==' operand                                                                      # EqualityExpression
    | operand '!=' operand                                                                      # InequalityExpression
    // Arithmetic Expressions
    | operand '+' operand                                                                       # AddExpression
    | operand '-' operand                                                                       # SubExpression
    | operand '*' operand                                                                       # MulExpression
    | operand '/' operand                                                                       # DivExpression
    | operand '%' operand                                                                       # ModExpression
    | operand '&' operand                                                                       # IntAndExpression
    | operand '|' operand                                                                       # IntOrExpression
    | operand '^' operand                                                                       # IntXorExpression
    | '~' operand                                                                               # IntNotExpression
    | operand '<<' operand                                                                      # IntLShiftExpression
    | operand '>>' operand                                                                      # IntRShiftExpression
    // Base Operand
    | operand                                                                                   # OperandExpression
    ;

variableDefinition
    : stringOperand '=' expression
    ;

expressionMapping
    : expression '=>' expression
    ;

operand
    : numberOperand
    | booleanOperand
    | stringOperand
    | listConstant
    | orderedMapConstant
    | variable
    | '$.' pathOrMetadata
    | '(' expression ')'
    ;

numberOperand: intOperand | floatOperand;

intOperand: INT;
floatOperand: FLOAT;

INT: '-'?[0-9]+;
FLOAT: '-'? [0-9]+ '.' [0-9]+;

booleanOperand: TRUE | FALSE;

TRUE: 'true';
FALSE: 'false';

stringOperand: QUOTED_STRING;

QUOTED_STRING: ('\'' (~'\'')* '\'') | ('"' (~'"')* '"');

listConstant: '[' operand? (',' operand)* ']';

orderedMapConstant: '{' mapPairConstant? (',' mapPairConstant)* '}';

mapPairConstant: mapKeyOperand ':' operand;

mapKeyOperand: intOperand | stringOperand;

variable: VARIABLE_REFERENCE;

VARIABLE_REFERENCE
    : '${' STRING_VARIABLE_NAME '}'
    ;

fragment STRING_VARIABLE_NAME
    : [a-zA-Z_][a-zA-Z0-9_]*
    ;

pathOrMetadata: path | metadata;

path: basePath ('.' pathFunction)?;

basePath: binPart ('.' (mapPart | listPart))*?;

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

PATH_FUNCTION_GET: 'get';

pathFunctionParamName
    : PATH_FUNCTION_PARAM_TYPE
    | PATH_FUNCTION_PARAM_RETURN
    ;

PATH_FUNCTION_PARAM_TYPE: 'type';

PATH_FUNCTION_PARAM_RETURN: 'return';

pathFunctionParamValue: pathFunctionGetType | pathFunctionReturnType;

pathFunctionGetType
    : 'INT'
    | 'STRING'
    | 'HLL'
    | 'BLOB'
    | 'FLOAT'
    | 'BOOL'
    | 'LIST'
    | 'MAP'
    | 'GEO'
    ;

pathFunctionReturnType: PATH_FUNCTION_CDT_RETURN_TYPE;

PATH_FUNCTION_CDT_RETURN_TYPE
    : 'VALUE'
    | 'KEY_VALUE'
    | 'UNORDERED_MAP'
    | 'ORDERED_MAP'
    | 'KEY'
    | 'INDEX'
    | 'RANK'
    | 'COUNT'
    | 'NONE'
    | 'EXISTS'
    | 'REVERSE_INDEX'
    | 'REVERSE_RANK'
    ;

binPart: NAME_IDENTIFIER;

mapPart
    : MAP_TYPE_DESIGNATOR
    | mapKey
    | mapValue
    | mapRank
    | mapIndex
    | mapKeyRange
    | mapKeyList
    | mapIndexRange
    | mapValueList
    | mapValueRange
    | mapRankRange
    | mapRankRangeRelative
    | mapIndexRangeRelative
    ;

MAP_TYPE_DESIGNATOR: '{}';

mapKey
    : NAME_IDENTIFIER
    | QUOTED_STRING
    ;

mapValue: '{=' valueIdentifier '}';

mapRank: '{#' INT '}';

mapIndex: '{' INT '}';

mapKeyRange
    : standardMapKeyRange
    | invertedMapKeyRange
    ;

standardMapKeyRange
    : '{' keyRangeIdentifier '}'
    ;

invertedMapKeyRange
    : '{!' keyRangeIdentifier '}'
    ;

keyRangeIdentifier
    : mapKey '-' mapKey
    | mapKey '-'
    ;

mapKeyList
    : standardMapKeyList
    | invertedMapKeyList
    ;

standardMapKeyList
    : '{' keyListIdentifier '}'
    ;

invertedMapKeyList
    : '{!' keyListIdentifier '}'
    ;

keyListIdentifier
    : mapKey (',' mapKey)*
    ;

mapIndexRange
    : standardMapIndexRange
    | invertedMapIndexRange
    ;

standardMapIndexRange
    : '{' indexRangeIdentifier '}'
    ;

invertedMapIndexRange
    : '{!' indexRangeIdentifier '}'
    ;

indexRangeIdentifier
    : start ':' end
    | start ':'
    ;

start: INT | '-' INT;
end: INT | '-' INT;

mapValueList
    : standardMapValueList
    | invertedMapValueList
    ;

standardMapValueList
    : '{=' valueListIdentifier '}'
    ;

invertedMapValueList
    : '{!=' valueListIdentifier '}'
    ;

mapValueRange
    : standardMapValueRange
    | invertedMapValueRange
    ;

standardMapValueRange
    : '{=' valueRangeIdentifier '}'
    ;

invertedMapValueRange
    : '{!=' valueRangeIdentifier '}'
    ;

valueRangeIdentifier
    : valueIdentifier ':' valueIdentifier
    | valueIdentifier ':'
    ;

mapRankRange
    : standardMapRankRange
    | invertedMapRankRange
    ;

standardMapRankRange
    : '{#' rankRangeIdentifier '}'
    ;

invertedMapRankRange
    : '{!#' rankRangeIdentifier '}'
    ;

rankRangeIdentifier
    : start ':' end
    | start ':'
    ;

mapRankRangeRelative
    : standardMapRankRangeRelative
    | invertedMapRankRangeRelative
    ;

standardMapRankRangeRelative
    : '{#' rankRangeRelativeIdentifier '}'
    ;

invertedMapRankRangeRelative
    : '{!#' rankRangeRelativeIdentifier '}'
    ;

rankRangeRelativeIdentifier
    : start ':' relativeRankEnd
    ;

relativeRankEnd
    : end relativeValue
    | relativeValue
    ;

relativeValue
    : '~' valueIdentifier
    ;

mapIndexRangeRelative
    : standardMapIndexRangeRelative
    | invertedMapIndexRangeRelative
    ;

standardMapIndexRangeRelative
    : '{' indexRangeRelativeIdentifier '}'
    ;

invertedMapIndexRangeRelative
    : '{!' indexRangeRelativeIdentifier '}'
    ;

indexRangeRelativeIdentifier
    : start ':' relativeKeyEnd
    ;

relativeKeyEnd
    : end '~' mapKey
    | '~' mapKey
    ;

listPart
    : LIST_TYPE_DESIGNATOR
    | listIndex
    | listValue
    | listRank
    | listIndexRange
    | listValueList
    | listValueRange
    | listRankRange
    | listRankRangeRelative
    ;

LIST_TYPE_DESIGNATOR: '[]';

listIndex: '[' INT ']';

listValue: '[=' valueIdentifier ']';

listRank: '[#' INT ']';

listIndexRange
    : standardListIndexRange
    | invertedListIndexRange
    ;

standardListIndexRange
    : '[' indexRangeIdentifier ']'
    ;

invertedListIndexRange
    : '[!' indexRangeIdentifier ']'
    ;

listValueList
    : standardListValueList
    | invertedListValueList
    ;

standardListValueList
    : '[=' valueListIdentifier ']'
    ;

invertedListValueList
    : '[!=' valueListIdentifier ']'
    ;

listValueRange
    : standardListValueRange
    | invertedListValueRange
    ;

standardListValueRange
    : '[=' valueRangeIdentifier ']'
    ;

invertedListValueRange
    : '[!=' valueRangeIdentifier ']'
    ;

listRankRange
    : standardListRankRange
    | invertedListRankRange
    ;

standardListRankRange
    : '[#' rankRangeIdentifier ']'
    ;

invertedListRankRange
    : '[!#' rankRangeIdentifier ']'
    ;

listRankRangeRelative
    : standardListRankRangeRelative
    | invertedListRankRangeRelative
    ;

standardListRankRangeRelative
    : '[#' rankRangeRelativeIdentifier ']'
    ;

invertedListRankRangeRelative
    : '[!#' rankRangeRelativeIdentifier ']'
    ;

valueIdentifier
    : NAME_IDENTIFIER
    | QUOTED_STRING
    | INT
    | '-' INT
    ;

valueListIdentifier: valueIdentifier ',' valueIdentifier (',' valueIdentifier)*;

pathFunction
    : pathFunctionCast
    | pathFunctionExists
    | pathFunctionGet
    | pathFunctionCount
    | 'remove' '()'
    | 'insert' '()'
    | 'set' '()'
    | 'append' '()'
    | 'increment' '()'
    | 'clear' '()'
    | 'sort' '()'
    ;

pathFunctionCast: PATH_FUNCTION_CAST;

PATH_FUNCTION_CAST
    : 'asInt()'
    | 'asFloat()'
    ;

pathFunctionExists: PATH_FUNCTION_EXISTS;

PATH_FUNCTION_EXISTS: 'exists' '()';

pathFunctionCount: PATH_FUNCTION_COUNT;

PATH_FUNCTION_COUNT: 'count' '()';

pathFunctionGet
    : PATH_FUNCTION_GET '(' pathFunctionParams ')'
    | PATH_FUNCTION_GET '()'
    ;

pathFunctionParams: pathFunctionParam (',' pathFunctionParam)*?;

pathFunctionParam: pathFunctionParamName ':' pathFunctionParamValue;

NAME_IDENTIFIER: [a-zA-Z0-9_]+;

WS: [ \t\r\n]+ -> skip;