package com.abhioncbr.kafka.connect.listener;

import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class SalesforceEventLoggingListener implements ClientSessionChannel.MessageListener {
    private static final Logger log = LoggerFactory.getLogger(SalesforceEventLoggingListener.class);

    private boolean logSuccess;
    private boolean logFailure;

    public SalesforceEventLoggingListener(boolean logSuccess, boolean logFailure) {
        this.logSuccess = logSuccess;
        this.logFailure = logFailure;
    }

    @Override
    public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
        if (logSuccess && message.isSuccessful()) {
            log.info(printPrefix() +  "Success:[" + clientSessionChannel.getId() + "]" + message);
        }

        if (logFailure && !message.isSuccessful()) {
            log.info(printPrefix() +  "Failure:[" + clientSessionChannel.getId() + "]" + message);
        }
    }

    private String printPrefix() {
        return "[" + timeNow() + "] ";
    }

    private String timeNow() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date now = new Date();
        return dateFormat.format(now);
    }
}
