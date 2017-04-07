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
        boolean outputAsJson = false;
        
        // parse command-line arguments to get bootstrap.servers and group.id
        Options options = new Options();
        try {
            options.addOption(Option.builder("b").longOpt("bootstrap-server").hasArg().argName("bootstrap-server")
                    .desc("Server to connect to <host:9092>").required().build());
            options.addOption(Option.builder("g").longOpt("group").hasArg().argName("group").desc(
                    "The consumer group we wish to act on").required().build());
            options.addOption(Option.builder("J").longOpt("json").argName("outputAsJson").desc(
                    "Output the data as json").build());
            CommandLine line = new DefaultParser().parse(options, args);
            bootstrapServer = line.getOptionValue("bootstrap-server");
            group = line.getOptionValue("group");
            outputAsJson = line.hasOption("json");
        } catch (ParseException ex) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ConsumerGroupLag", "Consumer Group Lag", options, ex.toString(), true);
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
            
            Map<TopicPartition, ConsumerSummary> whoOwnsPartition = new HashMap<TopicPartition, ConsumerSummary>();
            
            for (ConsumerSummary cs : csList) {
                scala.collection.immutable.List<TopicPartition> scalaAssignment = cs.assignment();
                List<TopicPartition> assignment = scala.collection.JavaConversions.seqAsJavaList(scalaAssignment);
                
                for (TopicPartition tp : assignment) {
                    whoOwnsPartition.put(tp, cs);
                }
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

                partitionMap.put("logStartOffset", begin);
                partitionMap.put("logEndOffset", end);
                partitionMap.put("partition", tp.partition());
                
                if (offsetAndMetadata == null) {
                    // no committed offsets
                    partitionMap.put("currentOffset", "unknown");
                } else {
                    partitionMap.put("currentOffset", offsetAndMetadata.offset());
                }

                long committed;
                if (offsetAndMetadata == null) {
                    // no committed offsets
                    partitionMap.put("lag", "unknown");
                } else {
                    partitionMap.put("lag", end - offsetAndMetadata.offset());
                    committed = offsetAndMetadata.offset();
                }
                ConsumerSummary cs = whoOwnsPartition.get(tp);
                partitionMap.put("consumerId", cs.consumerId());
                partitionMap.put("host", cs.host());
                partitionMap.put("clientId", cs.clientId());
            }
            
            if (outputAsJson) {
                ObjectMapper mapper = new ObjectMapper();
                String jsonInString = mapper.writeValueAsString(results);
                System.out.println(jsonInString);
            } else {
                System.out.format("%-30s %-30s %-10s %-15s %-15s %-15s %s", "GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "OWNER");
                System.out.println();
                for (String topic : results.keySet()) {
                    Map<Integer, Map<String, Object>> partitionToAssignmentInfo = results.get(topic);
                    for (int partition : partitionToAssignmentInfo.keySet()) {
                        Map<String, Object> assignment = partitionToAssignmentInfo.get(partition);
                        Object currentOffset = assignment.get("currentOffset");
                        long logEndOffset = (long) assignment.get("logEndOffset");
                        Object lag = assignment.get("lag");
                        String host = (String) assignment.get("host");
                        String clientId = (String) assignment.get("clientId");
                        System.out.format("%-30s %-30s %-10s %-15s %-15s %-15s %s_%s", group, topic, partition, currentOffset, logEndOffset, lag, clientId, host);
                        System.out.println();
                    }
                }
            }
            System.exit(0);
        }
    }
}