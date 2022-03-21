grammar Sql;

import Exp;

sql: (statement ';' )+ ;

statement : inputStatement | transformStatement | outputStatement;

inputStatement:  jdbcInput;

jdbcInput: 'create' 'table' tableName=IDENTIFIER 'using' 'jdbc' '(' property (',' property)* ')';

transformStatement: 'create' 'view'  viewName=IDENTIFIER 'as' sqlStatement;

sqlStatement: selectClause whereClause? groupClause? orderClause? limitClause?;

selectClause: 'select' projection (',' projection)* 'from' tableName=IDENTIFIER;

whereClause: 'where' predicate;

groupClause: 'group' 'by' expr (',' expr)*;

orderClause: 'order' 'by' expr (',' expr)*;

limitClause: 'limit' LONG;

projection: namedExpr | expr;

outputStatement: 'output'  source=IDENTIFIER ('(' property (',' property) ')')? 'as' target=IDENTIFIER;

property: propertyKey=IDENTIFIER '=' propertyValue=STRING;

WS: [ \r\n\t]+ -> channel(HIDDEN);