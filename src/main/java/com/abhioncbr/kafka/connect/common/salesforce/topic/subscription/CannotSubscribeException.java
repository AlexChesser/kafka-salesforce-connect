package com.abhioncbr.kafka.connect.common.salesforce.topic.subscription;

import java.net.URL;

public class CannotSubscribeException extends Exception {
    private final String topic;
    private final long replayFrom;
    private final Object error;
    private final URL endpoint;

    public CannotSubscribeException(URL endpoint, String topic, long replayFrom, Object error) {
        super(String.format("Unable to subscribe to [%s:%s] [%s] : %s", topic, replayFrom, endpoint, error));
        this.endpoint = endpoint;
        this.topic = topic;
        this.replayFrom = replayFrom;
        this.error = error;
    }

    public String getTopic() {
        return topic;
    }

    public long getReplayFrom() {
        return replayFrom;
    }

    public Object getError() {
        return error;
    }

    public URL getEndpoint() {
        return endpoint;
    }
}
