/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.TXT file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */
package com.github.jcustenborder.kafka.connect.common.salesforce.connector;


import com.github.jcustenborder.kafka.connect.common.salesforce.login.BayeuxParameters;
import com.github.jcustenborder.kafka.connect.common.salesforce.topic.subscription.TopicSubscription;
import com.github.jcustenborder.kafka.connect.common.salesforce.topic.subscription.TopicSubscriptionImpl;
import com.github.jcustenborder.kafka.connect.common.salesforce.util.SalesforceUtil;
import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author hal.hildebrand
 * @since API v37.0
 */
public class EmpConnector {
    private static final Logger log = LoggerFactory.getLogger(EmpConnector.class);

    private volatile BayeuxClient client;
    private final HttpClient httpClient;
    private final BayeuxParameters parameters;

    private final AtomicBoolean running = new AtomicBoolean();
    private AtomicBoolean reauthenticate = new AtomicBoolean(false);

    private final ConcurrentMap<String, Long> replay = new ConcurrentHashMap<>();
    private final Set<MessageListenerInfo> listenerInfos = new CopyOnWriteArraySet<>();
    private final Set<TopicSubscriptionImpl> subscriptions = new CopyOnWriteArraySet<>();

    private Function<Boolean, String> bearerTokenProvider;

    public EmpConnector(BayeuxParameters parameters) {
        this.parameters = parameters;
        httpClient = new HttpClient(parameters.sslContextFactory());
        httpClient.getProxyConfiguration().getProxies().addAll(parameters.proxies());
    }

    /**
     * Start the connector.
     * @return true if connection was established, false otherwise
     */
    public Future<Boolean> start() {
        if (running.compareAndSet(false, true)) {
            addListener(Channel.META_CONNECT, new AuthFailureListener());
            addListener(Channel.META_HANDSHAKE, new AuthFailureListener());
            replay.clear();
            return connect();
        }
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        future.complete(true);
        return future;
    }

    /**
     * Stop the connector
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (client != null) {

            // stopping the subscribed topics.
            subscriptions.forEach(subs -> {
                subs.cancel(client, running, replay);
                subscriptions.remove(subs);
            } );

            log.info("Disconnecting Bayeux Client in EmpConnector");
            client.disconnect();
            client = null;
        }
        if (httpClient != null) {
            try {
                httpClient.stop();
            } catch (Exception e) {
                log.error("Unable to stop HTTP transport[{}]", parameters.endpoint(), e);
            }
        }
    }

    /**
     * Set a bearer token / session id provider function that takes a boolean as input and returns a valid token.
     * If the input is true, the provider function is supposed to re-authenticate with the Salesforce server
     * and get a fresh session id or token.
     *
     * @param bearerTokenProvider a bearer token provider function.
     */
    public void setBearerTokenProvider(Function<Boolean, String> bearerTokenProvider) {
        this.bearerTokenProvider = bearerTokenProvider;
    }

    /**
     * Subscribe to a topic, receiving events after the replayFrom position
     *
     * @param topic
     *            - the topic to subscribe to
     * @param replayFrom
     *            - the replayFrom position in the event stream
     * @param consumer
     *            - the consumer of the events
     * @return a Future returning the Subscription - on completion returns a Subscription or throws a CannotSubscribe
     *         exception
     */
    public Future<TopicSubscription> subscribe(String topic, long replayFrom, Consumer<Map<String, Object>> consumer) {
        if (!running.get()) {
            throw new IllegalStateException(String.format("Connector[%s} has not been started",
                    parameters.endpoint()));
        }
        topic = topic.replaceAll("/$", "");

        final String topicWithoutQueryString = SalesforceUtil.topicWithoutQueryString(topic);
        if (replay.putIfAbsent(topicWithoutQueryString, replayFrom) != null) {
            throw new IllegalStateException(String.format("Already subscribed to %s [%s]",
                    topic, parameters.endpoint()));
        }

        TopicSubscriptionImpl subscription = new TopicSubscriptionImpl(topic, consumer);
        subscriptions.add(subscription);

        return subscription.subscribe(client, replay, parameters);
    }

    /**
     * Subscribe to a topic, receiving events from the earliest event position in the stream
     *
     * @param topic
     *            - the topic to subscribe to
     * @param consumer
     *            - the consumer of the events
     * @return a Future returning the Subscription - on completion returns a Subscription or throws a CannotSubscribe
     *         exception
     */
    public Future<TopicSubscription> subscribeEarliest(String topic, Consumer<Map<String, Object>> consumer) {
        return subscribe(topic, SalesforceUtil.REPLAY_FROM_EARLIEST, consumer);
    }

    /**
     * Subscribe to a topic, receiving events from the latest event position in the stream
     *
     * @param topic
     *            - the topic to subscribe to
     * @param consumer
     *            - the consumer of the events
     * @return a Future returning the Subscription - on completion returns a Subscription or throws a CannotSubscribe
     *         exception
     */
    public Future<TopicSubscription> subscribeTip(String topic, Consumer<Map<String, Object>> consumer) {
        return subscribe(topic, SalesforceUtil.REPLAY_FROM_TIP, consumer);
    }

    public EmpConnector addListener(String channel, ClientSessionChannel.MessageListener messageListener) {
        listenerInfos.add(new MessageListenerInfo(channel, messageListener));
        return this;
    }

    public boolean isConnected() {
        return client != null && client.isConnected();
    }

    public boolean isDisconnected() {
        return client == null || client.isDisconnected();
    }

    public boolean isHandshook() {
        return client != null && client.isHandshook();
    }

    public long getLastReplayId(String topic) {
        return replay.get(topic);
    }

    private Future<Boolean> connect() {
        log.info("EmpConnector connecting");
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        try {
            httpClient.start();
        } catch (Exception e) {
            log.error("Unable to start HTTP transport[{}]", parameters.endpoint(), e);
            running.set(false);
            future.complete(false);
            return future;
        }

        String bearerToken = bearerToken();
        LongPollingTransport httpTransport = new LongPollingTransport(parameters.longPollingOptions(), httpClient) {
            @Override
            protected void customize(Request request) {
                request.header(SalesforceUtil.AUTHORIZATION, bearerToken);
            }
        };

        client = new BayeuxClient(parameters.endpoint().toExternalForm(), httpTransport);
        client.addExtension(new ReplayExtension(replay));

        addListeners(client);

        client.handshake((c, m) -> {
            if (!m.isSuccessful()) {
                Object error = m.get(SalesforceUtil.ERROR);
                if (error == null) {
                    error = m.get(SalesforceUtil.FAILURE);
                }
                future.completeExceptionally(new ConnectException(
                        String.format("Cannot connect [%s] : %s", parameters.endpoint(), error)));
                running.set(false);
            } else {
                subscriptions.forEach(subs -> subs.subscribe(client, replay, parameters));
                future.complete(true);
            }
        });

        return future;
    }

    private void addListeners(BayeuxClient client) {
        for (MessageListenerInfo info : listenerInfos) {
            client.getChannel(info.getChannelName()).addListener(info.getMessageListener());
        }
    }

    private String bearerToken() {
        String bearerToken;
        if (bearerTokenProvider != null) {
            bearerToken = bearerTokenProvider.apply(reauthenticate.get());
            reauthenticate.compareAndSet(true, false);
        } else {
            bearerToken = parameters.bearerToken();
        }

        return bearerToken;
    }

    private void reconnect() {
        if (running.compareAndSet(false, true)) {
            connect();
        } else {
            log.error("The current value of running is not as we expect, this means our reconnection may not happen");
        }
    }

    /**
     * Listens to /meta/connect channel messages and handles 401 errors, where client needs
     * to reauthenticate.
     */
    private class AuthFailureListener implements ClientSessionChannel.MessageListener {
        private static final String ERROR_401 = "401";
        private static final String ERROR_403 = "403";

        @Override
        public void onMessage(ClientSessionChannel channel, Message message) {
            if (!message.isSuccessful()) {
                if (isError(message, ERROR_401) || isError(message, ERROR_403)) {
                    reauthenticate.set(true);
                    stop();
                    reconnect();
                }
            }
        }

        private boolean isError(Message message, String errorCode) {
            String error = (String)message.get(Message.ERROR_FIELD);
            String failureReason = getFailureReason(message);

            return (error != null && error.startsWith(errorCode)) ||
                    (failureReason != null && failureReason.startsWith(errorCode));
        }

        private String getFailureReason(Message message) {
            String failureReason = null;
            Map<String, Object> ext = message.getExt();
            if (ext != null) {
                Map<String, Object> sfdc = (Map<String, Object>)ext.get("sfdc");
                if (sfdc != null) {
                    failureReason = (String)sfdc.get("failureReason");
                }
            }
            return failureReason;
        }
    }

    private static class MessageListenerInfo {
        private String channelName;
        private ClientSessionChannel.MessageListener messageListener;

        MessageListenerInfo(String channelName, ClientSessionChannel.MessageListener messageListener) {
            this.channelName = channelName;
            this.messageListener = messageListener;
        }

        String getChannelName() {
            return channelName;
        }

        ClientSessionChannel.MessageListener getMessageListener() {
            return messageListener;
        }
    }
}
