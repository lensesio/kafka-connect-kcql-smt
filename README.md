# Kafka Connect Kcql Single Message Transform

Apply SQL like syntax to morph the Kafka message(key or/and value) when using Kafka Connect.

## Why 
Sources or sinks might produce/deal-with data that is not in sync with what you want:
 - you have a kafka topic where you want to pick up specific fields for the sink
 - you might want to flatten the message structure 
 - you might want to rename fields
 - (coming soon) might want to filter messages
 
And you want to express it with a simple syntax! This is where KCQL SMT comes to help you!

**0.1 (2017-05-...)**

* first release

### Building

***Requires gradle 3.4.1 to build.***

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