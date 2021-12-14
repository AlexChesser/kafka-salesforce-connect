package com.abhioncbr.kafka.connect.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CDCMessageHelper {
    private static final String REPLAY_ID = "replayId";
    private static final String RECORD_IDS = "recordIds";
    private static final String ENTITY_NAME = "entityName";
    private static final String CHANGE_EVENT_HEADER = "ChangeEventHeader";

    @SuppressWarnings("unchecked")
    public static ArrayList<String> getTopicEntity(Map<String, Object> payloadMap) {
        AtomicReference<String> entity = new AtomicReference<>();
        AtomicReference<ArrayList<String>> recordIds = new AtomicReference<>();
        payloadMap.forEach((payloadMapK, payloadMapV) -> {

            if(payloadMapK.equalsIgnoreCase(CHANGE_EVENT_HEADER)) {
                ObjectMapper oMapper = new ObjectMapper();
                Map<String, Object > payload = oMapper.convertValue(payloadMapV, Map.class);
                payload.forEach((headerK, headerV) -> {
                    if(headerK.equalsIgnoreCase(ENTITY_NAME)) entity.set(headerV.toString());
                    if(headerK.equalsIgnoreCase(RECORD_IDS)) {
                        ArrayList<String> rec = oMapper.convertValue(headerV, ArrayList.class);
                        recordIds.set(rec);
                    }
                });
            }

        });
        ArrayList<String> output = new ArrayList<>();
        output.add(entity.get()); output.addAll(recordIds.get());
        return output;
    }

    public static Long getReplayId(Map<String, Object> eventMap) {
        AtomicReference<Long> entity = new AtomicReference<>();
        eventMap.forEach((eventMapK, eventMapV) -> {
            if(eventMapK.equalsIgnoreCase(REPLAY_ID)) entity.set(Long.valueOf(eventMapV.toString()));
        });
        return entity.get();
    }
}
