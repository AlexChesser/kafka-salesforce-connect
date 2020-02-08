package com.abhioncbr.kafka.connect.common.salesforce.util;

public class SalesforceUtil {
    public static final String ERROR = "error";
    public static final String FAILURE = "failure";
    public static final String AUTHORIZATION = "Authorization";

    public static long REPLAY_FROM_EARLIEST = -2L;
    public static long REPLAY_FROM_TIP = -1L;

    public static String topicWithoutQueryString(String fullTopic) {
        return fullTopic.split("\\?")[0];
    }
}
