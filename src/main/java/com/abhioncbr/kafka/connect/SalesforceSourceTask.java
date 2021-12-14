package com.abhioncbr.kafka.connect;

import com.abhioncbr.kafka.connect.common.SourceRecordConcurrentLinkedDeque;
import com.abhioncbr.kafka.connect.common.salesforce.connector.EmpConnector;
import com.abhioncbr.kafka.connect.common.salesforce.login.BayeuxParameters;
import com.abhioncbr.kafka.connect.common.salesforce.login.BearerTokenProvider;
import com.abhioncbr.kafka.connect.listener.SalesforceEventLoggingListener;
import com.abhioncbr.kafka.connect.listener.TopicChannelMessageListener;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.cometd.bayeux.Channel.*;

public class SalesforceSourceTask extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(SalesforceSourceTask.class);
  private static final String SEPERATOR = ",";

  private EmpConnector connector;
  private SalesforceSourceConnectorConfig config;
  private final SourceRecordConcurrentLinkedDeque messageQueue = new SourceRecordConcurrentLinkedDeque();

  @Override
  public String version() {
    return config != null ? config.version: "0.1";
  }

  @Override
  public void start(Map<String, String> map) {
    log.debug("[Class:: SalesforceSourceTask]: starting 'start' method");
    this.config = new SalesforceSourceConnectorConfig(map);

    try {
      BearerTokenProvider tokenProvider = BearerTokenProvider.getToken(config.customURL, config.username, config.password);
      BayeuxParameters params = tokenProvider.login();

      connector = new EmpConnector(params);
      connector.setBearerTokenProvider(tokenProvider);

      // enable Salesforce success and failure login.
      enableLogging();

      // start Salesforce connector.
      connector.start().get(5, TimeUnit.SECONDS);

      String[] eventTopics = config.connectType == ConnectType.PUSH? config.pushTopics.split(SEPERATOR) : config.cdcTopics.split(SEPERATOR);
      for(String eventTopic: eventTopics) new TopicChannelMessageListener(eventTopic, connector, messageQueue, config);

    } catch (TimeoutException timeoutException) {
      throw new SourceTaskException(timeoutException, "[Timed out subscribing] :" + timeoutException.getCause().toString());
    } catch (Exception allOtherException) {
      throw new SourceTaskException(allOtherException, allOtherException.getCause().toString());
    }

    log.debug("[Class:: SalesforceSourceTask]: ending 'start' method");
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> records = new ArrayList<>(1024);
    while (!this.messageQueue.drain(records, 1000)) {}
    return records;
  }

  @Override
  public void stop() {
    this.connector.stop();
  }

  private void enableLogging() {
    if (this.connector != null){
      SalesforceEventLoggingListener loggingListener = new SalesforceEventLoggingListener(config.sfLogSuccess, config.sfLogFailure);
      connector.addListener(META_UNSUBSCRIBE, loggingListener).addListener(SERVICE, loggingListener)
              .addListener(META_HANDSHAKE, loggingListener).addListener(META_CONNECT, loggingListener)
              .addListener(META_DISCONNECT, loggingListener).addListener(META_SUBSCRIBE, loggingListener);
    }
  }


}