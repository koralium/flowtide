lexer grammar SpicedbLexer;

DEFINITION: 'definition';
RELATION: 'relation';
PERMISSION: 'permission';
CAVEAT: 'caveat';

COLON: ':';
STAR: '*';
PLUS: '+';
AND: '&';
POINTER: '->';
LEFT_CURLY: '{';
RIGHT_CURLY: '}';
EQUALS: '=';
PIPE: '|';
HASH: '#';
LEFT_PAREN: '(';
RIGHT_PAREN: ')';
COMMA: ',';
QUESTION: '?';
OR_PIPES: '||';
DOUBLE_AND: '&&';
SMALLER_THAN: '<';
GREATER_THAN: '>';
SMALLER_THAN_OR_EQUAL: '<=';
GREATER_THAN_OR_EQUAL: '>=';
NOT_EQUAL: '!=';
EQUAL: '==';
IN: 'in';
DASH: '-';
DIVIDE: '/';
MOD: '%';
EXCLAMATION: '!';
DOT: '.';
LEFT_SQUARE: '[';
RIGHT_SQUARE: ']';
NULL: 'null';
TRUE: 'true';
FALSE: 'false';
WITH: 'with';

IDENTIFIER: [a-zA-Z_] [a-zA-Z_0-9]*;

LineComment
    : '//' ~[\r\n]* -> channel(HIDDEN)
    ;

BlockComment
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

SPACES: [ \u000B\t\r\n] -> channel(HIDDEN);

STRING_LITERAL: '\'' ( ~'\'' | '\'\'')* '\'';

NUMERIC_LITERAL:
	'-'? ((DIGIT+ ('.' DIGIT*)?) | ('.' DIGIT+)) ('E' [-+]? DIGIT+)?;

fragment DIGIT: [0-9];