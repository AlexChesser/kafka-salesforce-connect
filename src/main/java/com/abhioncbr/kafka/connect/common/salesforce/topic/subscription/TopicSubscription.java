package com.abhioncbr.kafka.connect.common.salesforce.topic.subscription;

import com.abhioncbr.kafka.connect.common.salesforce.login.BayeuxParameters;
import org.cometd.client.BayeuxClient;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A subscription to a topic
 *
 * @author hal.hildebrand
 * @since API v37.0
 */
public interface TopicSubscription {

    /**
     * Cancel the subscription
     */
    void cancel(BayeuxClient client, AtomicBoolean running, ConcurrentMap<String, Long> replay);

    /**
     * @return the current replayFrom event id of the subscription
     */
    long getReplayFrom(ConcurrentMap<String, Long> replay);

    /**
     * @return the topic subscribed to
     */
    String getTopic();

    Future<TopicSubscription> subscribe(BayeuxClient client,
                                               ConcurrentMap<String, Long> replay, BayeuxParameters parameters);

}
