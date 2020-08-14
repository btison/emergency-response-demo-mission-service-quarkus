package com.redhat.emergency.response.source;

import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Message;

public class MessageWithAck<T> implements Message<T> {

    private final T payload;

    private boolean acked;

    private MessageWithAck(T payload) {
        this.payload = payload;
    }

    static <T> MessageWithAck<T> of(T payload) {
        return new MessageWithAck<>(payload);
    }

    @Override
    public CompletionStage<Void> ack() {
        acked = true;
        return getAck().get();
    }

    @Override
    public T getPayload() {
        return payload;
    }

    public boolean acked() {
        return acked;
    }
}
