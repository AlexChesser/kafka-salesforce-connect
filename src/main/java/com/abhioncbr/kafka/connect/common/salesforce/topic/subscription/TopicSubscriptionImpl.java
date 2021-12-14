package com.abhioncbr.kafka.connect.common.salesforce.topic.subscription;

import com.abhioncbr.kafka.connect.common.salesforce.login.BayeuxParameters;
import com.abhioncbr.kafka.connect.common.salesforce.util.SalesforceUtil;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class TopicSubscriptionImpl implements TopicSubscription {
    private final String topic;
    private final Consumer<Map<String, Object>> consumer;

    public TopicSubscriptionImpl(String topic, Consumer<Map<String, Object>> consumer) {
        this.topic = topic;
        this.consumer = consumer;
    }

    /*
     * (non-Javadoc)
     * @see com.salesforce.emp.connector.Subscription#cancel()
     */
    @Override
    public void cancel(BayeuxClient client, AtomicBoolean running, ConcurrentMap<String, Long> replay) {
        replay.remove(SalesforceUtil.topicWithoutQueryString(topic));
        if (running.get() && client != null) {
            client.getChannel(topic).unsubscribe();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.salesforce.emp.connector.Subscription#getReplay()
     */
    @Override
    public long getReplayFrom(ConcurrentMap<String, Long> replay) {
        return replay.getOrDefault(SalesforceUtil.topicWithoutQueryString(topic), SalesforceUtil.REPLAY_FROM_EARLIEST);
    }

    /*
     * (non-Javadoc)
     * @see com.salesforce.emp.connector.Subscription#getTopic()
     */
    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return String.format("Subscription [%s]", getTopic());
    }

    @Override
    public Future<TopicSubscription> subscribe(BayeuxClient client,
                                               ConcurrentMap<String, Long> replay, BayeuxParameters parameters) {
        long replayFrom = getReplayFrom(replay);
        ClientSessionChannel channel = client.getChannel(topic);
        CompletableFuture<TopicSubscription> future = new CompletableFuture<>();
        channel.subscribe((c, message) -> consumer.accept(message.getDataAsMap()), (c, message) -> {
            if (message.isSuccessful()) {
                future.complete(this);
            } else {
                Object error = message.get(SalesforceUtil.ERROR);
                if (error == null) {
                    error = message.get(SalesforceUtil.FAILURE);
                }
                future.completeExceptionally(
                        new CannotSubscribeException(parameters.endpoint(), topic, replayFrom, error != null ? error : message));
            }
        });
        return future;
    }
}
