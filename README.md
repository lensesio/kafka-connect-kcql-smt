![Action Status](https://github.com/lensesio/kafka-connect-kcql-smt/workflows/badge.svg)

# Kafka Connect Sql Single Message Transform

Use SQL to drive the transformation of the Kafka message(key or/and value) when using Kafka Connect.
Before SMT you needed a KStream app to take the message from the source topic apply the transformation to a new topic. 
We have developed a KStreams library ( you can find on [github](https://github.com/Landoop/kstreams-kcql)) to make it easy expressing simple Kafka streams transformations. 

*However the extra topic is not required anymore for Kafka Connect!*.

## Why 
Sources or sinks might produce/deal-with data that is not in sync with what you want:
 - you have a kafka topic where you want to pick up specific fields for the sink
 - you might want to flatten the message structure 
 - you might want to rename fields
 - (coming soon) might want to filter messages
 
And you want to express it with a simple syntax! This is where SQL SMT comes to help you!

## Configuration

| Configuration  | Type   | Required|Description | 
|----------------|--------|----------|------------|
| connect.transforms.sql.key  | String | N |Comma separated SQL targeting the key of a Kafka Message|
| connect.transforms.sql.value| String | N |Comma separated SQL targeting the value of a Kafka Message|

The SQL will define the mapping between the topic and the transformation to be applied. 
Each message on the specified topics will get the appropriate transformation.

Example configuration
```json
connect.transforms.sql.value=SELECT ingredients.name as fieldName,ingredients.*, ingredients.sugar as fieldSugar FROM topic1 withstructure;SELECT name, address.street.name as streetName, address.street2.name as streetName2 FROM topic2
```

## Kafka Connect Payloads supported
In most cases the payload sent over Kafka is Avro. That might not always be the case for existing systems where in most cases the payload is json. 
As a result the transform is capable of handling more than just Avro. 
Supported payload type (applies to both key and value):

| Schema Type  | Input  | Schema Output | Output | 
|--------------|--------|---------------|--------|
| Type.STRUCT  | Struct | Type Struct   | Struct |
| Type.BYTES   | Json (byte[])   | Schema.Bytes  | Json(byte[]) | 
| Type.STRING  | Json(string)   | Schema.STRING | Json (string)| 
| NULL  |  Json (byte[])| NULL | Json (byte[])|
| NULL  |  Json (string)| NULL | Json (string)|

### SQL
We make use of **Apache Calcite** to handle the SQL parsing. The library support for SQL is quite large but for now
we only handle simple SQL identifiers (nested structure is supported) with more to come like: WHERE condition and probably SQL operation(field concatenation for example)
Syntax:
```SQL
SELECT ...
FROM TOPIC
[WITHSTRUCTURE]
```

There are two modes for the SQL when it comes to Kafka Connect SMT
* flatten the structure. General syntax is like this:`SELECT ... FROM TOPIC_A`
```sql
//rename and only pick fields on first level
SELECT calories as C ,vegan as V ,name as fieldName FROM topic

//Cherry pick fields on different levels in the structure
SELECT name, address.street.name as streetName FROM topic

//Select and rename fields on nested level
SELECT name, address.street.*, address.street2.name as streetName2 FROM topic
```
* retain structure. Syntax looks like `SELECT ... FROM TOPIC_A WITHSTUCTURE`. Notice the **WITHSTRUCTRE** keyword. 
   
```sql
//you can select itself - obviously no real gain on this
SELECT * FROM topic withstructure 

//rename a field 
SELECT *, name as fieldName FROM topic withstructure

//rename a complex field
SELECT *, ingredients as stuff FROM topic withstructure

//select a single field
SELECT vegan FROM topic withstructure

//rename and only select nested fields
SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure
```


#### Not supported
Applying SQL to value to use the message key fields or metadata. Coming soon!

**2.0 (2020-01-01)**

* Updated to scala 2.12

**0.1 (2017-05-16)**

* first release

### Building

***Requires gradle 5.0 to build.***

To build

```bash
gradle compile
```

To test

```bash
gradle test
```


You can also use the gradle wrapper

```
./gradlew build
```

To view dependency trees

```
gradle dependencies # 
```

