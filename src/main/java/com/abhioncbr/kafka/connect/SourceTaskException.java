package com.abhioncbr.kafka.connect;

public class SourceTaskException extends RuntimeException {
    public SourceTaskException(Throwable throwable, String message) {
        super(message, throwable);
    }
}
