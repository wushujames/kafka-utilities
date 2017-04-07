package com.wushujames.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.admin.AdminClient;
import kafka.admin.AdminClient.ConsumerGroupSummary;
import kafka.admin.AdminClient.ConsumerSummary;

public class ConsumerGroupLag {

    public static void main(String[] args) throws JsonProcessingException {

        String bootstrapServer;
        String group;
        
        // parse command-line arguments to get bootstrap.servers and group.id
        Options options = new Options();
        try {
            options.addOption(Option.builder("b").longOpt("bootstrap-server").hasArg().argName("bootstrap-server")
                    .desc("Server to connect to <host:9092>").required().build());
            options.addOption(Option.builder("g").longOpt("group").hasArg().argName("group").desc(
                    "The consumer group we wish to act on").required().build());
            CommandLine line = new DefaultParser().parse(options, args);
            bootstrapServer = line.getOptionValue("bootstrap-server");
            group = line.getOptionValue("group");
        } catch (ParseException ex) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("consumer_lag_metrics", "Consumer Lag Metrics", options, ex.toString(), true);
            throw new RuntimeException("Invalid command line options.");
        }
        
        
        
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", group);
        
        Properties props2 = new Properties();
        props2.put("bootstrap.servers", bootstrapServer);
        AdminClient ac = AdminClient.create(props);
        /*
         * {
         *  "topic" : {
         *    "0" : {
         *            "log-start-offset" : 0,
         *            "log-end-offset" : 100,
         *            "current-offset" : 30,
         *            "partition" : 0 },
         * }
         */
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Collection<TopicPartition> c = new ArrayList<TopicPartition>();

            
            ConsumerGroupSummary summary = ac.describeConsumerGroup(group);

            scala.collection.immutable.List<ConsumerSummary> scalaList = summary.consumers().get();
            List<ConsumerSummary> csList = scala.collection.JavaConversions.seqAsJavaList(scalaList);
            for (ConsumerSummary cs : csList) {
                scala.collection.immutable.List<TopicPartition> scalaAssignment = cs.assignment();
                List<TopicPartition> assignment = scala.collection.JavaConversions.seqAsJavaList(scalaAssignment);
                c.addAll(assignment);
            }

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(c);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(c);
            
            Map<String, Map<Integer, Map<String, Object>>> results = new HashMap<String, Map<Integer, Map<String, Object>>>();
            consumer.assign(c);
            for (TopicPartition tp : c) {
                
                if (!results.containsKey(tp.topic())) {
                    results.put(tp.topic(), new HashMap<Integer, Map<String, Object>>());
                }
                Map<Integer, Map<String, Object>> topicMap = results.get(tp.topic());
                if (!topicMap.containsKey(tp.partition())) {
                    topicMap.put(tp.partition(), new HashMap<String, Object>());
                }
                Map<String, Object> partitionMap = topicMap.get(tp.partition());
                
                OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
                long end = endOffsets.get(tp);
                long begin = beginningOffsets.get(tp);

                partitionMap.put("log-start-offset", begin);
                partitionMap.put("log-end-offset", end);
                partitionMap.put("partition", tp.partition());
                

//                System.out.println("Begin: " + begin);
//                System.out.println("End: " + end);

                if (offsetAndMetadata == null) {
                    // no committed offsets
//                    System.out.println("Committed: unknown");
                    partitionMap.put("current-offset", "unknown");
                } else {
//                    System.out.println("Committed: " + offsetAndMetadata.offset());
                    partitionMap.put("current-offset", offsetAndMetadata.offset());
                }

                long committed;
                if (offsetAndMetadata == null) {
                    // no committed offsets
//                    System.out.println("Lag: " + (end - begin));
                } else {
                    committed = offsetAndMetadata.offset();
//                    System.out.println("Lag: " + (end-committed));
                }

//                System.out.println("");
                
            }
            ObjectMapper mapper = new ObjectMapper();
            String jsonInString = mapper.writeValueAsString(results);
            System.out.println(jsonInString);
            System.exit(0);

        }
    }
}