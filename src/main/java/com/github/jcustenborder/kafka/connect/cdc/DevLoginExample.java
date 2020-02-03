/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.TXT file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */
package com.github.jcustenborder.kafka.connect.cdc;


import com.github.jcustenborder.kafka.connect.common.salesforce.login.BayeuxParameters;
import com.github.jcustenborder.kafka.connect.common.salesforce.login.BearerTokenProvider;
import com.github.jcustenborder.kafka.connect.common.salesforce.login.LoginHelper;
import com.github.jcustenborder.kafka.connect.common.salesforce.topic.subscription.TopicSubscription;
import com.github.jcustenborder.kafka.connect.common.salesforce.connector.EmpConnector;
import com.github.jcustenborder.kafka.connect.common.salesforce.util.SalesforceUtil;
import org.eclipse.jetty.util.ajax.JSON;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.cometd.bayeux.Channel.*;

/**
 * An example of using the EMP connector
 *
 * @author hal.hildebrand
 * @since API v37.0
 */
public class DevLoginExample {

    public static void main(String[] argv) throws Throwable {
        if (argv.length < 4 || argv.length > 5) {
            System.err.println("Usage: DevLoginExample url username password topic [replayFrom]");
            System.exit(1);
        }
        Consumer<Map<String, Object>> consumer = event -> System.out.println(String.format("Received:\n%s", JSON.toString(event)));

        BearerTokenProvider tokenProvider = new BearerTokenProvider(() -> {
            try {
                return LoginHelper.login(new URL(argv[0]), argv[1], argv[2]);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        BayeuxParameters params = tokenProvider.login();

        EmpConnector connector = new EmpConnector(params);
        LoggingListener loggingListener = new LoggingListener(true, true);

        connector.addListener(META_HANDSHAKE, loggingListener)
                .addListener(META_CONNECT, loggingListener)
                .addListener(META_DISCONNECT, loggingListener)
                .addListener(META_SUBSCRIBE, loggingListener)
                .addListener(META_UNSUBSCRIBE, loggingListener);

        connector.setBearerTokenProvider(tokenProvider);

        connector.start().get(5, TimeUnit.SECONDS);

        long replayFrom = SalesforceUtil.REPLAY_FROM_EARLIEST;
/*        if (argv.length == 5) {
            replayFrom = Long.parseLong(argv[4]);
        }*/
        TopicSubscription[] subscription =  new TopicSubscription[2];
        try {
            subscription[0] = connector.subscribe(argv[3], replayFrom, consumer).get(5, TimeUnit.SECONDS);
            subscription[1] = connector.subscribe(argv[4], replayFrom, consumer).get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            System.err.println(e.getCause().toString());
            System.exit(1);
            throw e.getCause();
        } catch (TimeoutException e) {
            System.err.println("Timed out subscribing");
            System.exit(1);
            throw e.getCause();
        }

        System.out.println(String.format("Subscribed: %s", subscription[0]));
        System.out.println(String.format("Subscribed: %s", subscription[1]));
    }
}
