package com.abhioncbr.kafka.connect.listener;

import com.abhioncbr.kafka.connect.ConnectType;
import com.abhioncbr.kafka.connect.SalesforceSourceConnectorConfig;
import com.abhioncbr.kafka.connect.cdc.CDCMessageHelper;
import com.abhioncbr.kafka.connect.common.SourceRecordConcurrentLinkedDeque;
import com.abhioncbr.kafka.connect.common.salesforce.connector.EmpConnector;
import com.abhioncbr.kafka.connect.common.salesforce.util.SalesforceUtil;
import com.google.common.collect.ImmutableMap;
import com.abhioncbr.kafka.connect.push.PushTopicMessageHelper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class TopicChannelMessageListener {
  private static final Logger log = LoggerFactory.getLogger(TopicChannelMessageListener.class);
  private static final Map<String, ?> SOURCE_PARTITIONS = new HashMap<>();

  private final SalesforceSourceConnectorConfig config;
  private final SourceRecordConcurrentLinkedDeque messageQueue;

  public TopicChannelMessageListener(String topicName, EmpConnector connector,
                                     SourceRecordConcurrentLinkedDeque messageQueue, SalesforceSourceConnectorConfig config)
          throws InterruptedException, ExecutionException, TimeoutException {
    this.config = config;
    this.messageQueue = messageQueue;

    Consumer<Map<String, Object>> consumer = this::processMessage;

    // TODO: get from status kafka topic
    long replayFrom = SalesforceUtil.REPLAY_FROM_EARLIEST;
    connector.subscribe(topicName, replayFrom, consumer).get(config.connectTimeout, TimeUnit.SECONDS);
  }

  @SuppressWarnings("unchecked")
  public void processMessage(Map<String, Object> event) {
    AtomicReference<String> entity = new AtomicReference<>();
    AtomicReference<String> recordId = new AtomicReference<>();
    AtomicReference<Long> replayID = new AtomicReference<>();
    if(config.connectType == ConnectType.CDC) {
      event.forEach((eventK, eventV)-> {
        if(eventK.equalsIgnoreCase("payload")) {
          ArrayList<String> info = CDCMessageHelper.getTopicEntity((Map<String, Object>) eventV);
          if(info.size() >0) entity.set(info.get(0));
          if(info.size() >1) recordId.set(info.get(0));
        }

        if(eventK.equalsIgnoreCase("event")) {
          replayID.set(CDCMessageHelper.getReplayId((Map<String, Object>) eventV));
        }
      });
    } else{
      //TODO: implement for Push topics
      entity.set(PushTopicMessageHelper.getTopicEntity());
      replayID.set(PushTopicMessageHelper.getReplayId());
    }

    log.debug(entity.get() + "   " + replayID.get() + "  " + JSON.toString(event));

    String topicName = String.format(config.kafkaTopics, entity).toLowerCase();
    log.info("pushing an cdc event to topic: " + topicName);

    Map<String, String> sourceOffset = ImmutableMap.of("replayId", recordId.get());
    SourceRecord record = new SourceRecord(SOURCE_PARTITIONS, sourceOffset, topicName, null,
            Schema.INT64_SCHEMA, replayID.get(),
            Schema.STRING_SCHEMA, JSON.toString(event),
            System.currentTimeMillis());
    messageQueue.add(record);
  }
}