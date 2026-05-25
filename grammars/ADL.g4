grammar Dataflow;

//IMPORTANT: decide if engines are data/state or both

//cross engine edges leads to the creation of sinks/source
//es: 
//  DATA FILTER on spark streaming --> Aggregation on Flink
//becomes DATA FILTER ON SPARK --> kafka sink === kafka source --> AGGREGATION ON FLINK
// it must be held in mind when computing the costs

//we need to define schema for a stream

//with the current approach for map to state some cases aren't handled
//es: the state must be updated according to a custom FSM

//example:
/*dataflow TemperatureMonitoring {
  --producers
  producer SensorStream DATA frequency: high {
      room_id     : STRING_TYPE,
      temperature : DOUBLE,
      event_time  : TIMESTAMP
  }

  producer RoomRegistry STATE frequency: low {
      room_id   : STRING_TYPE,
      room_name : STRING_TYPE,
      floor     : LONG
  }

  --action components
  filter ValidReadings {
      temperature > 0.0 AND room_id IS NOT NULL
  }

  join Enriched {
      ValidReadings + RoomRegistry
      ON room_id = room_id
      TYPE LEFT
  }

  window MinuteWindow {
      "1 minute", event_time
  }

  aggregate WindowedAvg {
      GROUP BY room_id
      AVG(temperature) AS avg_temp
  }

  map_to_state CurrentState {
      KEY room_id VALUE temperature USING REPLACE
  }

  map_to_data HotAlerts {
      ISTREAM
  }

  --consumers
  consumer Dashboard DATA frequency: high delivery: at_most_once

  consumer StateStore STATE frequency: low delivery: at_least_once

  --edges
  edge SensorStream  -> ValidReadings
  edge ValidReadings -> Enriched
  edge RoomRegistry  -> Enriched
  edge Enriched      -> MinuteWindow
  edge MinuteWindow  -> WindowedAvg
  edge Enriched      -> CurrentState
  edge CurrentState  -> HotAlerts
  edge WindowedAvg   -> Dashboard
  edge CurrentState  -> StateStore
}
*/

/*
    The dataflow can be split across engines?
    Because if that is the case there is the need to create some "hidden" source/sinks to share data between engines (maybe this can affect the Cost evaluation)
    (could have a bigger impact in high frequency)
*/

/*
new pipeline proto to be more a graph rather than linear sequence of operations
message Pipeline {
  string        name  = 1;
  repeated Node nodes = 2; 
  repeated Edge edges = 3;
}

message Node {
  string id = 1;
  oneof kind {
    Source source = 2;
    Step   step   = 3;
    Sink   sink   = 4;
  }
}
 */

dataflow
    : DATAFLOW name LBRACE
        producer+
        actionComponent*
        consumer+
        edge+
      RBRACE
      EOF
    ;

//producers (sources)
producer
    : PRODUCER name type frequency schema
    ;

// schema block shared between producer and consumer
schema
    : LBRACE fieldDef (COMMA fieldDef)* RBRACE
    ;

fieldDef
    : name COLON typeName
    ;

//consumers (sinks)
consumer 
    : CONSUMER name type frequency delivery
    ;

//producer and consumer properties
type : DATA | STATE ;

frequency : FREQUENCY COLON frequencyVal ;
frequencyVal  : HIGH | MEDIUM | LOW ;

delivery  : DELIVERY COLON deliveryVal ;
deliveryVal   : AT_MOST_ONCE | AT_LEAST_ONCE | EXACTLY_ONCE ;


//action components (individual actions applied to data or state)
actionComponent
    : filterComponent
    | deriveComponent
    | selectComponent
    | aggregateComponent
    | windowComponent
    | joinComponent
    | mapToStateComponent
    | mapToDataComponent
    ;

//each component has a single action
//name is just a name that the user can assign to the component, it 
//is used later in the edge to specify the flow between components

//filter valid in both data and state 
//keep rows matching the expression, drop the others
//spark --> df.filter(...)
//flink --> stream.filter(...)
//MV --> WHERE ....
filterComponent
    : FILTER name LBRACE expression RBRACE
    ;

//adds or overwrite columns via expressions, schema grows, value change, nothing is dropped
//spark --> one or more df.withColumn(...) chained together
//flink --> map() with a row transform
//MV  --> SELECT (...) as ...
deriveComponent
    : DERIVE name LBRACE
        deriveExpr (COMMA deriveExpr)*
      RBRACE
    ;

deriveExpr
    : expression AS name
    ;

//keeps only the specified columns, it narrows the schema, other columns are dropped
//spark --> df.select(...)
//flink --> map() project only wanted fields
//MV  --> SELECT ...
selectComponent
    : SELECT name LBRACE
        selectExpr (COMMA selectExpr)*
      RBRACE
    ;

selectExpr
    : name
    | STAR
    ;

//window component
//should be valid in data mode only, this should be enforced in the
//semantics/parser
//SPARK --> df.grouypBy(window(...))
//flink stream.KeyBy(...).window(TumblingEventTimeWindows.of(...))
//MV --> ??
windowComponent
    : WINDOW name LBRACE
        duration = STRING COMMA timeCol=name
        (COMMA SLIDE slide=STRING)?
        //(WATERMARK watermarkDelay=STRING)?
      RBRACE
    ;

//aggregation component
//it contains a group by and aggregation function, is it correct to keep 
//SPark--> groupby(...).agg(...)
//requires update or complete output mode (in spark streaming)
//Flink --> keyBy(...).process(...)
//MV --> SELECT AVG(...) ... GROUP BY ...
aggregateComponent
    : AGGREGATE name LBRACE
        (GROUP BY name (COMMA name)*)?
        aggExpr (COMMA aggExpr)*
      RBRACE
    ;

//join component (merge)
//the two input names must match upstream node names declared via edges
//valid in data mode (stream stream or state-stream) and state mode (state -state)
//maybe full join type only available when both inputs are state? 

//spark --> df.join
//(streaming need watermarks for stream-stream joins)
//flink stream.join(...).where(...)
//MV --> join clause (state-state only?)
joinComponent
    : JOIN name LBRACE
        leftInput=name PLUS rightInput=name
        ON leftKey=name EQ rightKey=name
        TYPE joinType 
      RBRACE
    ;


//available on data, produces a state
// the keyCol represent the key that identifies the state
// the valueCol represent the value that represents the state,
//maybe the state is more complex than a single value, and we should allow
//the user to specify it or is it too much for now?
//spark df.groupBy(...).agg(...) write to external store
//flink KeyedStream.process(...)
//MV may require something to adapt stream to mat views
// INSERT ... ON CONFLICT DO ...

//replace this with aggregate + maptostate(replace)
mapToStateComponent
    : MAPTOSTATE name LBRACE
        KEY keyCol=name VALUE valueCol=name USING stateOperation
      RBRACE
    ;

stateOperation: REPLACE | INCREMENT | DECREMENT | MAXIMUM | MINIMUM | COLLECT ;

// agg (user_id, SUM(signed_amount)) maptostate(replace)
//maptostate(increment)

//map to data, valid on state, produces data
//stream operator are
//ISTREAM emit new state is added or a state is updated
//DSTREAM emit when a state entry is removed
//RSTERAM emit full current state immediately or on a schedule

//SparkStreaming cannot perform this, the input is state (not a stream)
//spark batch can perform this action
//  - RSTREAM read the state and emit all of it on a streaming sink (like kafka)
//  - DSTREAM and ISTREAM?
//      since batch is performed on a schedule, we could keep the old state
//      and compute the difference with the new state (at schedule trigger)
//      and emit the difference (deletions if DSTREAM or upserts if ISTREAM)
//flink?
mapToDataComponent
    : MAPTODATA name LBRACE
        streamOperator
      RBRACE
    ;


streamOperator: ISTREAM | DSTREAM | RSTREAM ;

//edges
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


// QUERIES
/*
1) From a sensor stream data telle me what is the current valid (>-5 and > 40)
temperature in each room
filter -> map_to_state
temperature > -5.0 AND temperature < 40.0

2)alert when the avg temp in a room exceeds 30 in the last 10 minutes
window -> aggregate -> filter -> select

3)show user balance from a stream of transactions

 derive SignedAmount {
      CASE type
          WHEN 'deposit'    THEN amount
          WHEN 'withdrawal' THEN amount * -1.0
          ELSE 0.0
      END AS signed_amount
  }

derive(signed_amount) -> aggregate(account_id, sum(signed_amount) as balance)-> MAPTOSTATE(account_id, balance, REPLACE)

OR

derive(signed_amount) ->maptostate(account_id, signed_amount, INCREMENT)
 */

// ── Keywords ──────────────────────────────────────────────────────────────────

DATAFLOW     : 'dataflow' ;
PRODUCER     : 'producer' ;
CONSUMER     : 'consumer' ;
FILTER       : 'filter' ;
SELECT       : 'select' ;
DERIVE       : 'derive' ;
WINDOW       : 'window' ;
AGGREGATE    : 'aggregate' ;
JOIN         : 'join' ;
MAPTOSTATE    : 'map_to_state' ;
MAPTODATA     : 'map_to_data' ;
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
DATA         : 'DATA' ;
STATE        : 'STATE' ;
HIGH         : 'high' ;
MEDIUM       : 'medium' ;
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
REPLACE      : 'REPLACE' ;
INCREMENT    : 'INCREMENT' ;
DECREMENT    : 'DECREMENT' ;
MAXIMUM      : 'MAXIMUM' ;
MINIMUM      : 'MINIMUM' ;
COLLECT      : 'COLLECT' ;
ISTREAM      : 'ISTREAM' ;
DSTREAM      : 'DSTREAM' ;
RSTREAM      : 'RSTREAM' ;
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

