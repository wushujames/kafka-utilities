# ConsumerGroupLag
A alternative to `kafka-consumer-groups.sh --describe` that:
1) is faster
2) works across Kafka versions (tested against brokers 0.10.0.0, 0.10.1.1, 0.10.2.0, 0.11.0.1)
3) can also output the information as JSON (Useful for piping into scripts)

Timing on a consumer group that consumes 180 partitions:
```
kafka-consumer-groups.sh   22 seconds
ConsumerGroupLag            1 second

Timing on a consumer group that consumes 800 partitions from a remote Kafka cluster:
kafka-consumer-groups.sh   230 seconds
ConsumerGroupLag             7 seconds
```

By default, it outputs in a format that is (mostly) compatible with kafka-consumer-groups.sh from Kafka 0.10.1.1

```
$ java -jar ConsumerGroupLag.jar --bootstrap-server broker.example.com:9092 --group wushujames-1491549464
GROUP                              TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             OWNER
wushujames-1491549464              some-topic 0          unknown         531220534       unknown         consumer-1_/192.168.1.1
wushujames-1491549464              some-topic 1          523573472       523573472       0               consumer-1_/192.168.1.1
wushujames-1491549464              some-topic 2          521553458       521553468       10              consumer-1_/192.168.1.1
```

If you add `--include-start-offset`, it adds an additional column that shows the first offset in each partition
```
$ java -jar ConsumerGroupLag.jar --bootstrap-server broker.example.com:9092 --group wushujames-1491549464 --include-start-offset
GROUP                              TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             OWNER                    LOG-START-OFFSET
wushujames-1491549464              some-topic 0          unknown         531220534       unknown         consumer-1_/192.168.1.1  0
wushujames-1491549464              some-topic 1          523573472       523573472       0               consumer-1_/192.168.1.1  0
wushujames-1491549464              some-topic 2          521553458       521553468       10              consumer-1_/192.168.1.1  0
```

If you add `--json` or `-J`, it will output the information as JSON. Useful for piping into scripts.
```
$ java -jar ConsumerGroupLag.jar --bootstrap-server broker.example.com:9092 --group wushujames-1491549464 --json
{
  "some-topic" : {
    "0" : {
      "logStartOffset" : 0,
      "logEndOffset" : 531145685,
      "partition" : 0,
      "currentOffset" : "unknown",
      "lag" : "unknown",
      "consumerId" : "consumer-1-f730ab1e-cdf0-41c4-b2c2-dd195ad40b4d",
      "host" : "/192.168.1.1",
      "clientId" : "consumer-1"
    },
    "1" : {
      "logStartOffset" : 0,
      "logEndOffset" : 523505602,
      "partition" : 1,
      "currentOffset" : 523505602,
      "lag" : 0,
      "consumerId" : "consumer-1-f730ab1e-cdf0-41c4-b2c2-dd195ad40b4d",
      "host" : "/192.168.1.1",
      "clientId" : "consumer-1"
    },
    "2" : {
      "logStartOffset" : 0,
      "logEndOffset" : 521484818,
      "partition" : 2,
      "currentOffset" : 521484808,
      "lag" : 10,
      "consumerId" : "consumer-1-f730ab1e-cdf0-41c4-b2c2-dd195ad40b4d",
      "host" : "/192.168.1.1",
      "clientId" : "consumer-1"
    }
  }
}
```

The schema for this is:
```
{
  "topic-name" : {
    "partiton-number" : {
      "logStartOffset" : ...
      "logEndOffset" : ...
      "partition" : ...
      "currentOffset" : ...
      "lag" : ...
      "consumerId" : ...
      "host" : ...
      "clientId" : ...
    },
    ...
  }
}
```

# Building

To build:
```
$ ./gradlew build
```

To build a fat jar with all dependencies included:
```
$ ./gradlew shadowJar
:compileJava
:processResources
:classes
:shadowJar

BUILD SUCCESSFUL

Total time: 2.494 secs
$ java -jar build/libs/ConsumerGroupLag-0.0.2-all.jar
```
