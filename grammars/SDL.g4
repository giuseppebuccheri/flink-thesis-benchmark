grammar SDL;

/*
usage example: Harri - operational reporting scenario

scenario HarriOperational {
    -- Producers (RDS, DynamoDB, API Gateway)
    producer RelationalDB frequency: daily { user_id: LONG, status: T_STRING }
    producer LiveClicks frequency: high { user_id: LONG, click_data: T_STRING }

    -- Action Components (ETL: Data Cleaner, Financial Enrichment, Daily Stats)
    -- Data Cleaning
    clean CleanedClicks { DROP NULLS }
    
    -- Enrichment (Join con i dati finanziari)
    join FinancialEnrichment { 
        LiveClicks + RelationalDB ON user_id = user_id TYPE LEFT 
    }
    
    -- aggregation for the reporting (daily stats)
    aggregate DailyStats { GROUP BY user_id COUNT(click_data) AS total_clicks }

    -- Consumers (Warehouse Exporter, Report Generator)
    consumer FastDashboard frequency: high delivery: at_most_once
    consumer ScheduledExporter frequency: daily delivery: exactly_once

    -- Edges
    edge LiveClicks -> CleanedClicks
    edge CleanedClicks -> FinancialEnrichment
    edge RelationalDB -> FinancialEnrichment
    edge FinancialEnrichment -> DailyStats
    edge DailyStats -> FastDashboard
    edge DailyStats -> ScheduledExporter
}
 */

// ── Root ──────────────────────────────────────────────────────────────────────
scenario
    : SCENARIO name LBRACE
        producer+
        actionComponent*
        consumer+
        edge+
      RBRACE
      EOF
    ;

// ── Producers & Consumers ───────────────────────────────
// no STATE or DATA

//producers (sources)
producer
    : PRODUCER name frequency schema
    ;

//consumers (sinks)
consumer 
    : CONSUMER name frequency delivery
    ;

// schema block shared between producer and consumer
schema
    : LBRACE fieldDef (COMMA fieldDef)* RBRACE
    ;

fieldDef
    : name COLON typeName
    ;

// Non-functional requirements (Business)
frequency : FREQUENCY COLON frequencyVal ;
frequencyVal  : HIGH | DAILY | LOW ;

delivery  : DELIVERY COLON deliveryVal ;
deliveryVal   : AT_MOST_ONCE | AT_LEAST_ONCE | EXACTLY_ONCE ;

// ── Action Components ────────────────────────────────────
actionComponent
    : filterComponent
    | deriveComponent
    | selectComponent
    | aggregateComponent
    | windowComponent
    | joinComponent
    | cleanComponent
    | patternMatchComponent
    ;


filterComponent
    : FILTER name LBRACE expression RBRACE
    ;


deriveComponent
    : DERIVE name LBRACE
        deriveExpr (COMMA deriveExpr)*
      RBRACE
    ;

deriveExpr
    : expression AS name
    ;

selectComponent
    : SELECT name LBRACE
        selectExpr (COMMA selectExpr)*
      RBRACE
    ;

selectExpr
    : name
    | STAR
    ;

windowComponent
    : WINDOW name LBRACE
        duration = STRING COMMA timeCol=name
        (COMMA SLIDE slide=STRING)?
      RBRACE
    ;

aggregateComponent
    : AGGREGATE name LBRACE
        (GROUP BY name (COMMA name)*)?
        aggExpr (COMMA aggExpr)*
      RBRACE
    ;

joinComponent
    : JOIN name LBRACE
        leftInput=name PLUS rightInput=name
        ON leftKey=name EQ rightKey=name
        TYPE joinType 
      RBRACE
    ;

// Data Cleaning
cleanComponent
    : CLEAN name LBRACE
        (DROP DUPLICATES)?
        (FILL NULLS WITH literal)?
        (DROP NULLS)?
      RBRACE
    ;

// Pattern Matching (Complex Event Processing)
patternMatchComponent
    : PATTERN name LBRACE
        MATCH patternString=STRING 
        DEFINE name AS expression (COMMA name AS expression)*
      RBRACE
    ;

// ── Data flow (Edges) ───────────────────────────────────────────────────────
edge
    : EDGE name ARROW name (LBRACKET scheduleProp  RBRACKET)?
    ;

scheduleProp
    : SCHEDULE COLON STRING
    ;

// ── Aggregation sub-rules ─────────────────────────────────────────────────────

aggExpr
    : aggFunc LPAREN (expression | STAR) RPAREN AS name
    ;

aggFunc
    : SUM | COUNT | AVG | MAX | MIN | LAST | FIRST
    | STDDEV | VARIANCE | COLLECT_LIST
    ;


joinType      : INNER | LEFT | LEFT_SEMI | FULL | FULL_OUTER ;
compOp        : EQ | NEQ | GT | LT | GTE | LTE ;
arithOp       : PLUS | MINUS | STAR | SLASH ;
typeName      : T_DOUBLE | T_LONG | T_STRING | T_BOOLEAN | T_TIMESTAMP ;
literal       : NUMBER # numLit | STRING # strLit | TRUE # trueLit | FALSE # falseLit ;
name          : ID | STRING ;

// ── Expression language ───────────────────────────────────────────────────────

expression
    : expression AND expression             # andExpr
    | expression OR expression              # orExpr
    | NOT expression                        # notExpr
    | expression compOp expression          # compExpr
    | expression arithOp expression         # arithExpr
    | expression BETWEEN expression AND expression # betweenExpr
    | expression IS NULL                    # isNullExpr
    | expression IS NOT NULL                # isNotNullExpr
    | CASE expression (WHEN expression THEN expression)+ (ELSE expression)? END          # simpleCaseExpr
    | CASE (WHEN expression THEN expression)+ (ELSE expression)? END    # searchedCaseExpr
    | builtinFunc                           # funcExpr
    | name                                  # colRef
    | literal                               # litExpr
    | LPAREN expression RPAREN              # parenExpr
    ;

builtinFunc
    : UPPER      LPAREN expression RPAREN
    | LOWER      LPAREN expression RPAREN
    | ROUND      LPAREN expression COMMA NUMBER RPAREN
    | ABS        LPAREN expression RPAREN
    | FLOOR      LPAREN expression RPAREN
    | CEIL       LPAREN expression RPAREN
    | CAST       LPAREN expression COMMA typeName RPAREN
    | LENGTH     LPAREN expression RPAREN
    | SUBSTRING  LPAREN expression COMMA NUMBER COMMA NUMBER RPAREN
    | TO_TIMESTAMP LPAREN expression COMMA STRING RPAREN
    | YEAR       LPAREN expression RPAREN
    | MONTH      LPAREN expression RPAREN
    | DAY        LPAREN expression RPAREN
    | HOUR       LPAREN expression RPAREN
    ;

// ── Keywords ──────────────────────────────────────────────────────────────────

SCENARIO     : 'scenario' ;
PRODUCER     : 'producer' ;
CONSUMER     : 'consumer' ;
FILTER       : 'filter' ;
SELECT       : 'select' ;
DERIVE       : 'derive' ;
WINDOW       : 'window' ;
AGGREGATE    : 'aggregate' ;
JOIN         : 'join' ;
EDGE         : 'edge' ;
AS           : 'AS' ;
ON           : 'ON' ;
BY           : 'BY' ;
GROUP        : 'GROUP' ;
KEY          : 'KEY' ;
VALUE        : 'VALUE' ;
USING        : 'USING' ;
TYPE         : 'TYPE' ;
SLIDE        : 'SLIDE' ;
WATERMARK    : 'WATERMARK' ;
SCHEDULE     : 'SCHEDULE' ;
PERSIST      : 'persist' ;
VOLATILE     : 'volatile' ;
FREQUENCY    : 'frequency' ;
DELIVERY     : 'delivery' ;
HIGH         : 'high' ;
DAILY       : 'daily' ;
LOW          : 'low' ;
AT_MOST_ONCE   : 'at_most_once' ;
AT_LEAST_ONCE  : 'at_least_once' ;
EXACTLY_ONCE   : 'exactly_once' ;
AND          : 'AND' ;
OR           : 'OR' ;
NOT          : 'NOT' ;
IS           : 'IS' ;
NULL         : 'NULL' ;
BETWEEN      : 'BETWEEN' ;
TRUE         : 'TRUE' ;
FALSE        : 'FALSE' ;
INNER        : 'INNER' ;
LEFT         : 'LEFT' ;
LEFT_SEMI    : 'LEFT_SEMI' ;
FULL         : 'FULL' ;
FULL_OUTER   : 'FULL_OUTER' ;
SUM          : 'SUM' ;
COUNT        : 'COUNT' ;
AVG          : 'AVG' ;
MAX          : 'MAX' ;
MIN          : 'MIN' ;
LAST         : 'LAST' ;
FIRST        : 'FIRST' ;
STDDEV       : 'STDDEV' ;
VARIANCE     : 'VARIANCE' ;
COLLECT_LIST : 'COLLECT_LIST' ;
UPPER        : 'UPPER' ;
LOWER        : 'LOWER' ;
ROUND        : 'ROUND' ;
ABS          : 'ABS' ;
FLOOR        : 'FLOOR' ;
CEIL         : 'CEIL' ;
CAST         : 'CAST' ;
COALESCE     : 'COALESCE' ;
LENGTH       : 'LENGTH' ;
SUBSTRING    : 'SUBSTRING' ;
CONCAT       : 'CONCAT' ;
TO_TIMESTAMP : 'TO_TIMESTAMP' ;
DATE_FORMAT  : 'DATE_FORMAT' ;
YEAR         : 'YEAR' ;
MONTH        : 'MONTH' ;
DAY          : 'DAY' ;
HOUR         : 'HOUR' ;
T_DOUBLE     : 'DOUBLE' ;
T_LONG       : 'LONG' ;
T_STRING     : 'STRING_TYPE' ;
T_BOOLEAN    : 'BOOLEAN' ;
T_TIMESTAMP  : 'TIMESTAMP' ;
CASE  : 'CASE' ;
WHEN  : 'WHEN' ;
THEN  : 'THEN' ;
ELSE  : 'ELSE' ;
END   : 'END' ;

CLEAN        : 'clean' ;
PATTERN      : 'pattern' ;
MATCH        : 'MATCH' ;
DEFINE       : 'DEFINE' ;
DROP         : 'DROP' ;
DUPLICATES   : 'DUPLICATES' ;
FILL         : 'FILL' ;
WITH         : 'WITH' ;
NULLS        : 'NULLS' ;

// New functions for spatial and temporal operations
DATEDIFF     : 'DATEDIFF' ;
TIME_WINDOW  : 'TIME_WINDOW' ;
ST_DISTANCE  : 'ST_DISTANCE' ;
ST_CONTAINS  : 'ST_CONTAINS' ;

// ── Operators and symbols ─────────────────────────────────────────────────────

EQ       : '=' ;    NEQ    : '!=' ;
GT       : '>' ;    LT     : '<' ;
GTE      : '>=' ;   LTE    : '<=' ;
PLUS     : '+' ;    MINUS  : '-' ;
STAR     : '*' ;    SLASH  : '/' ;
ARROW    : '->' ;
COMMA    : ',' ;
COLON    : ':' ;
LBRACE   : '{' ;    RBRACE   : '}' ;
LBRACKET : '[' ;    RBRACKET : ']' ;
LPAREN   : '(' ;    RPAREN   : ')' ;

STRING  : '\'' (~['\r\n])* '\'' | '"' (~["\r\n])* '"' ;
NUMBER  : '-'? [0-9]+ ('.' [0-9]+)? ;
ID      : [a-zA-Z_] [a-zA-Z0-9_\-]* ;

WS           : [ \t\r\n]+ -> skip ;
LINE_COMMENT : '--' ~[\r\n]* -> skip ;
BLOCK_COMMENT: '/*' .*? '*/' -> skip ;
