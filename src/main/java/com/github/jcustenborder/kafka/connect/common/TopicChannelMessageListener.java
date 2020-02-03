package com.github.jcustenborder.kafka.connect.common;

import com.github.jcustenborder.kafka.connect.common.salesforce.connector.EmpConnector;
import com.github.jcustenborder.kafka.connect.common.salesforce.util.SalesforceUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class TopicChannelMessageListener {
  private static final Logger log = LoggerFactory.getLogger(TopicChannelMessageListener.class);
  private static final Map<String, ?> SOURCE_PARTITIONS = new HashMap<>();

  private final SourceRecordConcurrentLinkedDeque messageQueue;

  public TopicChannelMessageListener(EmpConnector connector, SourceRecordConcurrentLinkedDeque messageQueue)
          throws InterruptedException, ExecutionException, TimeoutException {
    this.messageQueue = messageQueue;

    Consumer<Map<String, Object>> consumer = this::processMessage;
    long replayFrom = SalesforceUtil.REPLAY_FROM_EARLIEST;
    connector.subscribe("/data/AccountChangeEvent", replayFrom, consumer).get(5, TimeUnit.SECONDS);
  }

  public void processMessage(Map<String, Object> event) {
    AtomicReference<String> entity = new AtomicReference<>();
    AtomicReference<Long> replayID = new AtomicReference<>();
    event.forEach((eventK, eventV)-> {
        if(eventK.equalsIgnoreCase("payload")) {
          entity.set(getTopicEntity((Map<String, Object>) eventV));
        }

        if(eventK.equalsIgnoreCase("event")) {
          replayID.set(getReplayId((Map<String, Object>) eventV));
        }

    });
    System.out.println(entity.get() + "   " + replayID.get() + "  " + JSON.toString(event));

    Map<String, Long> sourceOffset = ImmutableMap.of("replayId", replayID.get());
    SourceRecord record = new SourceRecord(SOURCE_PARTITIONS, sourceOffset, entity.get(), null,
            Schema.INT64_SCHEMA, replayID.get(),
            Schema.STRING_SCHEMA, JSON.toString(event),
            System.currentTimeMillis());
    messageQueue.add(record);
  }

  private String getTopicEntity(Map<String, Object> payloadMap) {
    AtomicReference<String> entity = new AtomicReference<>();
    payloadMap.forEach((payloadMapK, payloadMapV) -> {

      if(payloadMapK.equalsIgnoreCase("ChangeEventHeader")) {
        ((Map<String, Object>) payloadMapV).forEach((headerK, headerV) -> {
          if(headerK.equalsIgnoreCase("entityName")) entity.set(headerV.toString());
        });
      }

    });
    return entity.get();
  }

  private Long getReplayId(Map<String, Object> eventMap) {
    AtomicReference<Long> entity = new AtomicReference<>();
    eventMap.forEach((eventMapK, eventMapV) -> {
      if(eventMapK.equalsIgnoreCase("replayId")) entity.set(Long.valueOf(eventMapV.toString()));
    });
    return entity.get();
  }
}