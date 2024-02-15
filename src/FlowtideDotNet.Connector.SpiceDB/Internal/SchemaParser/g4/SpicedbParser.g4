parser grammar SpicedbParser;


options {
	tokenVocab = SpicedbLexer;
}

parse: (block)* EOF;

def: 'definition';

block: 
  type_definition
  | caveat;

caveat: 'caveat' definition_name=IDENTIFIER '(' (caveat_parameter)? (',' caveat_parameter)*  ')' '{' (cel_expr)* '}';

caveat_parameter: name=IDENTIFIER parameter_type=IDENTIFIER;

type_definition: 'definition' definition_name=IDENTIFIER '{' (type_relation)* '}';

type_relation:
	relation=RELATION relation_name=IDENTIFIER ':' relation_type ('|' relation_type)*
	| permission=PERMISSION permission_name=IDENTIFIER '=' permission_userset=userset;

relation_type: name=IDENTIFIER ((':' star='*') | ('#' relation=IDENTIFIER))? ('with' caveat_name=IDENTIFIER)?;

userset:
	userset union='+' userset ('+' userset)*
	| userset intersect='&' userset ('&' userset)*
	| self_relation=IDENTIFIER tuple='->' object_relation=IDENTIFIER
	| compute=IDENTIFIER;

literal_value:
	NUMERIC_LITERAL
	| STRING_LITERAL
	| NULL
	| TRUE
	| FALSE;
	

cel_expr: cel_conditional_or ('?' cel_conditional_or ':' cel_expr)?;
cel_conditional_or: cel_conditional_and ('||' cel_conditional_and)*;
cel_conditional_and: cel_relation ('&&' cel_relation)*;
cel_relation: cel_addition (cel_relop cel_relation)*;
cel_relop: '<' | '<=' | '>=' | '>' | '==' | '!=' | 'in';
cel_addition: cel_multiplication (('+' | '-') cel_multiplication)*;
cel_multiplication: cel_unary (('*' | '/' | '%') cel_unary)*;
cel_unary:
	cel_member
	| '!' ('!')* cel_member
	| '-' ('-')* cel_member
	;
cel_member: 
	cel_primary
	| cel_member '.' IDENTIFIER ('(' (cel_expr_list)? ')')?
	| cel_member '[' cel_expr ']'
	;
cel_primary:
	('.')? IDENTIFIER ('(' (cel_expr_list)? ')')?
	| '(' cel_expr ')'
	| '[' cel_expr_list (',')? ']'
	| '{' cel_map_inits (',')? '}'
	| ('.')? IDENTIFIER ('.' IDENTIFIER)* '{' (cel_fields_init)? (',')? '}'
	| literal_value
	;
cel_expr_list: cel_expr (',' cel_expr)*;
cel_map_inits: cel_expr ':' cel_expr (',' cel_expr ':' cel_expr)*;
cel_fields_init: IDENTIFIER ':' cel_expr (',' IDENTIFIER ':' cel_expr)*;