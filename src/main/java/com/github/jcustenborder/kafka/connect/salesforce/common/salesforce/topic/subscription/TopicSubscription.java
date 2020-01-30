/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.TXT file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */
package com.github.jcustenborder.kafka.connect.salesforce.common.salesforce.topic.subscription;

import com.github.jcustenborder.kafka.connect.salesforce.common.salesforce.login.BayeuxParameters;
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
