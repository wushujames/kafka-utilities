package com.wushujames.kafka;

import kafka.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.IsolationLevel;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class TopicPartitionsOffsetInfo implements Runnable {

    private static final long END_OFFSET_VALUE = -1L;
    private static final long BEGINNING_OFFSET_VALUE = -2L;

    private static final Object endOffsetMapLock = new Object();
    private static final Object startOffsetMapLock = new Object();
    public static Map<TopicPartition, Long> endOffsetsMap = new HashMap<>();
    public static Map<TopicPartition, Long> beginningOffsetsMap = new HashMap<>();

    private final AdminClient adminClient;
    private final KafkaApiRequest kafkaApiRequest;

    private Map<Node,List<TopicPartition>> nodePartitionMap = new HashMap<>();
    private Node coordinator = null;
    private List<TopicPartition> topicPartitionList = new ArrayList<>();
    private CountDownLatch countDownLatch = null;
    private Node leaderNode = null;
    private long offsetRequestValue = -1L;

    TopicPartitionsOffsetInfo(final AdminClient adminClient){
        this.adminClient = adminClient;
        this.kafkaApiRequest = new KafkaApiRequest(adminClient.client());
    }

    TopicPartitionsOffsetInfo(final AdminClient adminClient, final Node leaderNode, final List<TopicPartition>topicPartitionList,
                              final CountDownLatch countDownLatch,
                              final long offsetRequestValue){
        this.adminClient = adminClient;
        this.kafkaApiRequest = new KafkaApiRequest(adminClient.client());
        this.topicPartitionList = topicPartitionList;
        this.countDownLatch = countDownLatch;
        this.leaderNode = leaderNode;
        this.offsetRequestValue = offsetRequestValue;
    }

    public void findCoordinatorNodeForGroup(final String groupName, final long retryTimeMs){
        this.coordinator = this.adminClient.findCoordinator(groupName, retryTimeMs);
    }

    public void storeTopicPartitionPerNodeInfo(final Properties props, final List<String> topicNamesList) throws OffsetFetchException {
        org.apache.kafka.clients.admin.AdminClient newAc = org.apache.kafka.clients.admin.AdminClient.create(props);
        DescribeTopicsResult describeTopicsResult = newAc.describeTopics(topicNamesList);
        try {
            Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
            for (Map.Entry<String, TopicDescription> entry : topicDescriptionMap.entrySet()) {
                for (TopicPartitionInfo partitionInfo : entry.getValue().partitions()) {
                    List<TopicPartition> topicPartitionList = nodePartitionMap.get(partitionInfo.leader());
                    if (topicPartitionList == null) {
                        topicPartitionList = new ArrayList<>();
                    }
                    TopicPartition tp = new TopicPartition(entry.getKey(), partitionInfo.partition());
                    topicPartitionList.add(tp);
                    nodePartitionMap.put(partitionInfo.leader(), topicPartitionList);
                }
            }
        } catch (InterruptedException e) {
            throw new OffsetFetchException("TopicPartitionsOffsetInfo InterruptedException:" + e.getMessage());
        } catch (ExecutionException e) {
            throw new OffsetFetchException("TopicPartitionsOffsetInfo ExecutionException:" + e.getMessage());
        }
    }

    public Map<TopicPartition, OffsetFetchResponse.PartitionData> getCommitedOffsets(final String groupName, final List<TopicPartition> topicPartitions,
                                                                                     final long responseWaitTime) throws OffsetFetchException {
        if(this.coordinator == null){
            throw new OffsetFetchException("Missing Group Coordinator for group:" + groupName);
        }

        OffsetFetchRequest.Builder offsetRequestBuilder =
                new OffsetFetchRequest.Builder(groupName, topicPartitions);
        this.kafkaApiRequest.sendApiRequest(this.coordinator, offsetRequestBuilder);
        OffsetFetchResponse offsetFetchResponse =(OffsetFetchResponse) this.kafkaApiRequest.getLastApiResponse(responseWaitTime);
        if(offsetFetchResponse.error() == Errors.NONE) {
            return offsetFetchResponse.responseData();
        }else{
            throw new OffsetFetchException(offsetFetchResponse.error().message());
        }
    }

    private void requestNprocessEndOffsets(){
        Map<TopicPartition, Long> requiredTimestamp = new HashMap<>();
        for(TopicPartition tp : this.topicPartitionList){
            requiredTimestamp.put(tp, TopicPartitionsOffsetInfo.END_OFFSET_VALUE);
        }
        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
                .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(requiredTimestamp);

        this.kafkaApiRequest.sendApiRequest(this.leaderNode, builder);
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse) this.kafkaApiRequest.getLastApiResponse(1000);
        this.addEndoffsets(this.processListOffsetResponse(listOffsetResponse, requiredTimestamp));
    }

    private void requestNprocessBeginningOffsets(){
        Map<TopicPartition, Long> requiredTimestamp = new HashMap<>();
        for(TopicPartition tp : this.topicPartitionList){
            requiredTimestamp.put(tp, TopicPartitionsOffsetInfo.BEGINNING_OFFSET_VALUE);
        }
        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
                .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(requiredTimestamp);

        this.kafkaApiRequest.sendApiRequest(this.leaderNode, builder);
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse) this.kafkaApiRequest.getLastApiResponse(1000);
        this.addBeginningOffsets(this.processListOffsetResponse(listOffsetResponse, requiredTimestamp));
    }

    private Map<TopicPartition, Long> processListOffsetResponse(final ListOffsetResponse listOffsetResponse, final Map<TopicPartition, Long>requiredTimestamp) {

        Map<TopicPartition, Long>processTopicPartitionOffsets = new HashMap<>();
        processTopicPartitionOffsets.putAll(requiredTimestamp);
        for (Map.Entry<TopicPartition, Long> entry : requiredTimestamp.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            ListOffsetResponse.PartitionData partitionData = listOffsetResponse.responseData().get(topicPartition);
            Errors error = partitionData.error;
            if (error == Errors.NONE) {
                //supporting kafka version greater than 10 only
                if (partitionData.offset != ListOffsetResponse.UNKNOWN_OFFSET) {
                    processTopicPartitionOffsets.put(topicPartition, partitionData.offset);
                }
            }
        }
        return processTopicPartitionOffsets;
    }

    private void addEndoffsets(Map<TopicPartition, Long> endOffsets){
        synchronized(endOffsetMapLock){
            TopicPartitionsOffsetInfo.endOffsetsMap.putAll(endOffsets);
        }
    }

    private void addBeginningOffsets(Map<TopicPartition, Long> endOffsets){
        synchronized(startOffsetMapLock){
            TopicPartitionsOffsetInfo.beginningOffsetsMap.putAll(endOffsets);
        }
    }

    public Map<TopicPartition, Long> getEndOffsets() throws OffsetFetchException {
        this.RequestPerBrokerPartitionOffsetDetails(TopicPartitionsOffsetInfo.END_OFFSET_VALUE);
        return TopicPartitionsOffsetInfo.endOffsetsMap;
    }

    public Map<TopicPartition, Long> getBeginningOffsets() throws OffsetFetchException {
        this.RequestPerBrokerPartitionOffsetDetails(TopicPartitionsOffsetInfo.BEGINNING_OFFSET_VALUE);
        return TopicPartitionsOffsetInfo.beginningOffsetsMap;
    }

    private void RequestPerBrokerPartitionOffsetDetails(final long value) throws OffsetFetchException {
        CountDownLatch countDownLatch = new CountDownLatch(nodePartitionMap.size());
        int threadCount = 0;
        for(Map.Entry<Node,List<TopicPartition>>nodeListEntry: nodePartitionMap.entrySet()){
            Thread newThread = new Thread(new TopicPartitionsOffsetInfo(this.adminClient, nodeListEntry.getKey(),nodeListEntry.getValue(),countDownLatch, value));
            newThread.setName("ThreadNumber:" + threadCount);   //Need to think on Executor framework.
            newThread.start();
            threadCount ++;
        }

        try {
            //wait for all threads to complete
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new OffsetFetchException("TopicPartitionsOffsetInfo RequestPerBrokerPartitionOffsetDetails InterruptedException:" + e.getMessage());
        }
    }

    @Override
    public void run() {
        if(this.offsetRequestValue == TopicPartitionsOffsetInfo.END_OFFSET_VALUE) {
            this.requestNprocessEndOffsets();
        }else {
            this.requestNprocessBeginningOffsets();
        }
        this.countDownLatch.countDown();
    }
}
