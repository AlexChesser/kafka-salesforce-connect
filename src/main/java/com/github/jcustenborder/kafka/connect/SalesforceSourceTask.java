/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect;

import com.github.jcustenborder.kafka.connect.common.SourceRecordConcurrentLinkedDeque;
import com.github.jcustenborder.kafka.connect.common.TopicChannelMessageListener;
import com.github.jcustenborder.kafka.connect.common.salesforce.login.BayeuxParameters;
import com.github.jcustenborder.kafka.connect.common.salesforce.login.LoginHelper;
import com.github.jcustenborder.kafka.connect.cdc.LoggingListener;
import com.github.jcustenborder.kafka.connect.common.salesforce.connector.EmpConnector;
import com.github.jcustenborder.kafka.connect.common.salesforce.login.BearerTokenProvider;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.cometd.bayeux.Channel.*;


public class SalesforceSourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(SalesforceSourceTask.class);

  private EmpConnector connector;
  private SalesforceSourceConnectorConfig config;
  private final SourceRecordConcurrentLinkedDeque messageQueue = new SourceRecordConcurrentLinkedDeque();

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = new SalesforceSourceConnectorConfig(map);

    String salesforceCustomURL = "https://traderev--nilam2.cs51.my.salesforce.com/";  //  /topic/AddressPushTopic
    String salesforceUsername =  "abhishek.sharma@traderev.app.nilam2"; //config.username;
    String salesforcePassword = "131188ScottWQjJJmM1wI583VDiIzUteiSV"; //config.password;

    BearerTokenProvider tokenProvider = new BearerTokenProvider(() -> {
      try {
        return LoginHelper.login(new URL(salesforceCustomURL), salesforceUsername, salesforcePassword);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    try {
      BayeuxParameters params = tokenProvider.login();
      EmpConnector connector = new EmpConnector(params);

      LoggingListener loggingListener = new LoggingListener(true, true);
      connector.addListener(META_HANDSHAKE, loggingListener).addListener(META_CONNECT, loggingListener)
            .addListener(META_DISCONNECT, loggingListener).addListener(META_SUBSCRIBE, loggingListener)
            .addListener(META_UNSUBSCRIBE, loggingListener);

      connector.setBearerTokenProvider(tokenProvider);
      connector.start().get(5, TimeUnit.SECONDS);

      new TopicChannelMessageListener(connector, messageQueue);
    } catch (ExecutionException e) {
      System.err.println(e.getCause().toString());
      System.exit(1);
    } catch (TimeoutException e) {
      System.err.println("Timed out subscribing");
      System.exit(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> records = new ArrayList<>(1024);
    while (!this.messageQueue.drain(records, 1000)) { }

    return records;
  }

  @Override
  public void stop() {
    this.connector.stop();
  }




}