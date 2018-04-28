package com.wushujames.kafka;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

public class KafkaApiRequest {

    final private ConsumerNetworkClient networkClient;
    private RequestFuture<ClientResponse> clientResponse = null;

    KafkaApiRequest(final ConsumerNetworkClient networkClient){
        this.networkClient = networkClient;
    }

    public void sendApiRequest(final Node node, final AbstractRequest.Builder<?> requestBuilder){
        this.clientResponse  = this.networkClient.send(node, requestBuilder);
    }

    public AbstractResponse getLastApiResponse(final long waitTimeMsBetweenCheckingResponse){

        while(!this.clientResponse.isDone()){
            try {
                Thread.sleep(waitTimeMsBetweenCheckingResponse);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return this.clientResponse.value().responseBody();
    }
}

