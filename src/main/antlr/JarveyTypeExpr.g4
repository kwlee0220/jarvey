grammar JarveyTypeExpr;

@header {
package jarvey.support.typeexpr;
}
	
columnExprList
	: columnExpr (',' columnExpr)*
	;
	
columnExpr
	: ID typeExpr ('not' 'null')?
	;
	
typeExpr
	: 'String'
	| 'Long'
	| 'Integer'
	| 'Int'
	| 'Short'
	| 'Byte'
	| 'Double'
	| 'Float'
	| 'Binary'
	| 'Boolean'
	| 'Date'
	| 'Timestamp'
	| 'CalendarInterval'
	| geomtryTypeExpr sridSpec?
	| 'Envelope'
	| 'Array' '<' typeExpr (',' nullSpec)? '>'
	;
	
geomtryTypeExpr
	: 'Geometry'
	| 'Point'
	| 'MultiPoint'
	| 'LineString'
	| 'MultiLineString'
	| 'Polygon'
	| 'MultiPolygon'
	| 'GeometryCollection'
	;
	
sridSpec
	: '(' INT ')'
	;
	
nullSpec
	: 'nullable'
	| 'non-null'
	| 'not-null'
	;
	
ID	:	ID_LETTER (ID_LETTER | DIGIT)* ;
INT :	DIGIT+;

fragment ID_LETTER :	'a'..'z'|'A'..'Z'|'_'|'/'|'$'| '\u0080'..'\ufffe' ;
fragment DIGIT :	'0'..'9' ;

STRING	:	'\'' (ESC|.)*? '\'' ;
fragment ESC :	'\\' [btnr"\\] ;

LINE_COMMENT:	'//' .*? '\n' -> skip ;
COMMENT		:	'/*' .*? '*/' -> skip ;

WS	:	[ \t\r\n]+ -> skip;