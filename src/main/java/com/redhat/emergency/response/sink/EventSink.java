package com.redhat.emergency.response.sink;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import javax.enterprise.context.ApplicationScoped;

import com.redhat.emergency.response.model.Mission;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class EventSink {

    private static final Logger log = LoggerFactory.getLogger(EventSink.class);

    private final UnicastProcessor<Pair<String, JsonObject>> missionProcessor = UnicastProcessor.create();

    private final UnicastProcessor<Pair<String, JsonObject>> responderProcessor = UnicastProcessor.create();

    public Uni<Void> missionStarted(Mission mission) {
        return missionEvent(mission, "MissionStartedEvent");
    }

    public Uni<Void> missionPickedUp(Mission mission) {
        return missionEvent(mission, "MissionPickedUpEvent");
    }

    public Uni<Void> missionCompleted(Mission mission) {
        return missionEvent(mission, "MissionCompletedEvent");
    }

    public Uni<Void> missionEvent(Mission mission, String type) {

        return Uni.createFrom().<Void>item(() -> {
            missionProcessor.onNext(ImmutablePair.of(mission.getIncidentId(),
                    initMessage(new JsonObject(), type).put("body", JsonObject.mapFrom(mission))));
            return null;
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    public Uni<Void> responderCommand(Mission mission, BigDecimal lat, BigDecimal lon, Boolean person) {
        return Uni.createFrom().<Void>item(() -> {
            responderProcessor.onNext(ImmutablePair.of(mission.getResponderId(),
                    initMessage(new JsonObject(), "UpdateResponderCommand")
                            .put("body", new JsonObject().put("responder", new JsonObject().put("id", mission.getResponderId())
                            .put("latitude", lat.doubleValue()).put("longitude", lon.doubleValue()).put("available", true)
                            .put("enrolled", !person)))));
            return null;
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Outgoing("mission-event")
    public Multi<Message<String>> missionEvent() {
        return missionProcessor.onItem().transform(p -> {
            log.debug("Sending message to mission-event channel. Key: " + p.getLeft() + " - Message = " + p.getRight().encode());
            return toMessage(p);
        });
    }

    @Outgoing("responder-command")
    public Multi<Message<String>> responderCommand() {
        return responderProcessor.onItem().transform(p -> {
            log.debug("Sending message to responder-command channel. Key: " + p.getLeft() + " - Message = " + p.getRight().encode());
            return toMessage(p);
        });
    }

    private Message<String> toMessage(Pair<String, JsonObject> keyPayloadPair) {
        return KafkaRecord.of(keyPayloadPair.getLeft(), keyPayloadPair.getRight().encode());
    }

    private JsonObject initMessage(JsonObject json, String messageType) {

        return json.put("id", UUID.randomUUID().toString())
                .put("invokingService", "MissionService")
                .put("timestamp", Instant.now().toEpochMilli())
                .put("messageType", messageType);
    }

}
