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
import org.apache.kafka.common.TopicPartition;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.admin.AdminClient;
import kafka.admin.AdminClient.ConsumerGroupSummary;
import kafka.admin.AdminClient.ConsumerSummary;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;


public class ConsumerGroupLag {

    static public class PartitionState {

        public long logStartOffset;
        public long logEndOffset;
        public int partition;
        public Object currentOffset;
        public Object lag;
        public String consumerId;
        public String host;
        public String clientId;

    }

    public static void main(String[] args) throws JsonProcessingException, OffsetFetchException {

        String bootstrapServer;
        String group;
        boolean outputAsJson = false;
        boolean includeStartOffset = false;
        Long groupStabilizationTimeoutMs = 5000L;
        Long waitForResponseTime = 1000L;


        // parse command-line arguments to get bootstrap.servers and group.id
        Options options = new Options();
        try {
            options.addOption(Option.builder("b").longOpt("bootstrap-server").hasArg().argName("bootstrap-server")
                    .desc("Server to connect to <host:9092>").required().build());
            options.addOption(Option.builder("g").longOpt("group").hasArg().argName("group").desc(
                    "The consumer group we wish to act on").required().build());
            options.addOption(Option.builder("J").longOpt("json").argName("outputAsJson").desc(
                    "Output the data as json").build());
            options.addOption(Option.builder("s").longOpt("include-start-offset").argName("includeStartOffset").desc(
                    "Include log-start-offset (the offset of the first record in a partition)").build());
            options.addOption(Option.builder("t").longOpt("group-stabilization-timeout")
                    .argName("groupStabilizationTimeoutMs")
                    .desc("time (ms) to wait for the consumer group description to be avaialble (e.g. wait for a consumer group rebalance to complete)")
                    .build());
            options.addOption(Option.builder("w").longOpt("response-wait-time")
                    .argName("waitForResponseTime")
                    .desc("time (ms) to wait for asking request response.")
                    .build());
            CommandLine line = new DefaultParser().parse(options, args);
            bootstrapServer = line.getOptionValue("bootstrap-server");
            group = line.getOptionValue("group");
            outputAsJson = line.hasOption("json");
            includeStartOffset = line.hasOption("s");
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
         *   "topic-name" : {
         *     "partition-number" : {
         *       "logStartOffset" : ...
         *       "logEndOffset" : ...
         *       "partition" : ...
         *       "currentOffset" : ...
         *       "lag" : ...
         *       "consumerId" : ...
         *       "host" : ...
         *       "clientId" : ...
         *     },
         *     ...
         *   }
         * }
         */
            Collection<TopicPartition> c = new ArrayList<TopicPartition>();


            ConsumerGroupSummary summary = ac.describeConsumerGroup(group, groupStabilizationTimeoutMs);

            if (summary.state().equals("Dead")) {
                System.out.format("Consumer group %s does not exist.", group);
                System.out.println();
                System.exit(1);
            }

            if (!summary.state().equals("Stable")) {
                // PreparingRebalance, AwaitingSync
                System.err.format("Warning: Consumer group %s has state %s.", group, summary.state());
                System.err.println();
            }

            scala.collection.immutable.List<ConsumerSummary> scalaList = summary.consumers().get();
            List<ConsumerSummary> csList = scala.collection.JavaConversions.seqAsJavaList(scalaList);
            if (csList.isEmpty()) {
                System.out.format("Consumer group %s is rebalancing.", group);
                System.out.println();
                System.exit(1);
            }

            Map<TopicPartition, ConsumerSummary> whoOwnsPartition = new HashMap<TopicPartition, ConsumerSummary>();
            List<String> topicNamesList = new ArrayList<String>();

            for (ConsumerSummary cs : csList) {
                scala.collection.immutable.List<TopicPartition> scalaAssignment = cs.assignment();
                List<TopicPartition> assignment = scala.collection.JavaConversions.seqAsJavaList(scalaAssignment);

                for (TopicPartition tp : assignment) {
                    whoOwnsPartition.put(tp, cs);
                    topicNamesList.add(tp.topic());
                }
                c.addAll(assignment);
            }

            //Get Per broker topic info based on topic list
            TopicPartitionsOffsetInfo topicPartitionsOffsetInfo = new TopicPartitionsOffsetInfo(ac);
            topicPartitionsOffsetInfo.storeTopicPartitionPerNodeInfo(props, topicNamesList);

            //Get endoffset and BeginningOffsets info for all topics
            Map<TopicPartition, Long> endOffsets = topicPartitionsOffsetInfo.getEndOffsets();
            Map<TopicPartition, Long> beginningOffsets = topicPartitionsOffsetInfo.getBeginningOffsets();

            //Get commited offset info
            topicPartitionsOffsetInfo.findCoordinatorNodeForGroup(group, waitForResponseTime);
            Map<TopicPartition, PartitionData> commitedOffsets = topicPartitionsOffsetInfo.getCommitedOffsets(group, (List<TopicPartition>) c, waitForResponseTime);

            Map<String, Map<Integer, PartitionState>> results = new HashMap<String, Map<Integer, PartitionState>>();

            for (TopicPartition tp : c) {

                if (!results.containsKey(tp.topic())) {
                    results.put(tp.topic(), new HashMap<Integer, PartitionState>());
                }
                Map<Integer, PartitionState> topicMap = results.get(tp.topic());
                if (!topicMap.containsKey(tp.partition())) {
                    topicMap.put(tp.partition(), new PartitionState());
                }
                PartitionState partitionMap = topicMap.get(tp.partition());

                PartitionData topicPartitionCommitedOffset = commitedOffsets.get(tp);
                //-1: No Info available
                if(topicPartitionCommitedOffset.offset == -1){
                    topicPartitionCommitedOffset = null;
                }

                long end = endOffsets.get(tp);
                long begin = beginningOffsets.get(tp);

                partitionMap.logStartOffset = begin;
                partitionMap.logEndOffset = end;
                partitionMap.partition = tp.partition();

                if (topicPartitionCommitedOffset == null) {
                    // no committed offsets
                    partitionMap.currentOffset = "unknown";
                } else {
                    partitionMap.currentOffset = topicPartitionCommitedOffset.offset;
                }

                if (topicPartitionCommitedOffset == null || end == -1) {
                    // no committed offsets
                    partitionMap.lag = "unknown";
                } else {
                    partitionMap.lag = end - topicPartitionCommitedOffset.offset;
                }
                ConsumerSummary cs = whoOwnsPartition.get(tp);
                partitionMap.consumerId = cs.consumerId();
                partitionMap.host = cs.host();
                partitionMap.clientId = cs.clientId();
            }

            if (outputAsJson) {
                ObjectMapper mapper = new ObjectMapper();
                String jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(results);
                System.out.println(jsonInString);
            } else {
                System.out.format("%-30s %-30s %-10s %-15s %-15s %-15s %-30s", "GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "OWNER");
                if (includeStartOffset) {
                    System.out.format(" %-15s", "LOG-START-OFFSET");
                }
                System.out.println();
                for (String topic : results.keySet()) {
                    Map<Integer, PartitionState> partitionToAssignmentInfo = results.get(topic);
                    for (int partition : partitionToAssignmentInfo.keySet()) {
                        PartitionState partitionState = partitionToAssignmentInfo.get(partition);
                        Object currentOffset = partitionState.currentOffset;
                        long logEndOffset = partitionState.logEndOffset;
                        Object lag = partitionState.lag;
                        String host = partitionState.host;
                        String clientId = partitionState.clientId;
                        System.out.format("%-30s %-30s %-10s %-15s %-15s %-15s %s_%s", group, topic, partition, currentOffset, logEndOffset, lag, clientId, host);
                        if (includeStartOffset) {
                            System.out.format(" %-15s", partitionState.logStartOffset);
                        }
                        System.out.println();
                    }
                }
            }
            System.exit(0);
    }
}
