package com.github.jcustenborder.kafka.connect.salesforce.push;

import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PushMessageListener implements ClientSessionChannel.MessageListener {
    private static final Logger log = LoggerFactory.getLogger(PushMessageListener.class);

    private boolean logSuccess;
    private boolean logFailure;

    //private final String channel = String.format("/topic/%s", this.config.salesForcePushTopicName);
    private final ConcurrentMap<String, Long> replay = new ConcurrentHashMap<>();

    public PushMessageListener() {
        this.logSuccess = true;
        this.logFailure = true;
    }

    public PushMessageListener(boolean logSuccess, boolean logFailure) {
        this.logSuccess = logSuccess;
        this.logFailure = logFailure;
    }

    @Override
    public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
        if (logSuccess && message.isSuccessful()) {
            System.out.println(">>>>");
            printPrefix();
            System.out.println("Success:[" + clientSessionChannel.getId() + "]");
            System.out.println(message);
            System.out.println("<<<<");
        }

        if (logFailure && !message.isSuccessful()) {
            System.out.println(">>>>");
            printPrefix();
            System.out.println("Failure:[" + clientSessionChannel.getId() + "]");
            System.out.println(message);
            System.out.println("<<<<");
        }
    }

    private void printPrefix() {
        System.out.print("[" + timeNow() + "] ");
    }

    private String timeNow() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date now = new Date();
        return dateFormat.format(now);
    }

   /* @Override
    public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
        log.info("onMessage(META_HANDSHAKE) - {}", message);

        if (message.isSuccessful()) {
            if (null == clientSessionChannel) {
                log.trace("onMessage(META_HANDSHAKE) - This is the first call to the topic channel.");
                clientSessionChannel = streamingClient.getChannel(channel);
            }
            setReplayId(channel);
            if (clientSessionChannel.getSubscribers().isEmpty()) {
                log.info("onMessage(META_HANDSHAKE) - Subscribing to {}", channel);
                clientSessionChannel.subscribe(topicChannelListener);
            } else {
                log.warn("onMessage(META_HANDSHAKE) - Already subscribed.");
            }
        } else {
            log.error("Error during handshake: {} {}", message.get("error"), message.get("exception"));
        }
    }

    private void setReplayId(final String channel) {
        replay.clear();
        OffsetStorageReader offsetReader = context.offsetStorageReader();
        Map<String, Object> partitionOffset = offsetReader.offset(new HashMap<>());
        if (partitionOffset != null && partitionOffset.get("replayId") instanceof Long) {
            Long sourceOffset = (Long) partitionOffset.get("replayId");
            log.info("PushTopic {} - found stored offset {}", config.salesForcePushTopicName, sourceOffset);
            replay.put(channel, sourceOffset);
        }
    }*/
}
