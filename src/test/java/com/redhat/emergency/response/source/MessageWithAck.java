package com.redhat.emergency.response.source;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import io.vertx.kafka.client.consumer.impl.KafkaReadStreamImpl;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class MessageWithAck<K, T> extends IncomingKafkaRecord<K, T> {

    private final T payload;

    private boolean acked;

    public MessageWithAck(KafkaConsumerRecord<K, T> record, KafkaCommitHandler commitHandler, KafkaFailureHandler onNack, T payload) {
        super(record, commitHandler, onNack);
        this.payload = payload;
    }

    static <K, T> MessageWithAck<K, T> of(K key, T payload) {
        KafkaConsumer<K, T> c = new KafkaConsumer<>(new KafkaConsumerImpl(new KafkaReadStreamImpl<K, T>(null, null)));
        ConsumerRecord<K, T> cr = new ConsumerRecord<>("topic", 1, 100, key, payload);
        KafkaConsumerRecord<K, T> kcr = new KafkaConsumerRecord<>(new KafkaConsumerRecordImpl<>(cr));
        return new MessageWithAck<K, T>(kcr, null, null, payload);
    }

    @Override
    public CompletionStage<Void> ack() {
        acked = true;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public T getPayload() {
        return payload;
    }

    public boolean acked() {
        return acked;
    }
}
