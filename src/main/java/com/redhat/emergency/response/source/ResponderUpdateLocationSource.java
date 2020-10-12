package com.redhat.emergency.response.source;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.emergency.response.model.Mission;
import com.redhat.emergency.response.model.MissionStatus;
import com.redhat.emergency.response.model.ResponderLocationHistory;
import com.redhat.emergency.response.model.ResponderLocationStatus;
import com.redhat.emergency.response.repository.MissionRepository;
import com.redhat.emergency.response.sink.EventSink;
import com.redhat.emergency.response.tracing.TracingKafkaUtils;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ResponderUpdateLocationSource {

    @Inject
    MissionRepository repository;

    @Inject
    EventSink eventSink;

    @Inject
    Tracer tracer;

    private static final Logger log = LoggerFactory.getLogger(ResponderUpdateLocationSource.class);

    @Incoming("responder-location-update")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Uni<CompletionStage<Void>> process(Message<String> responderLocationUpdate) {

        Span span = TracingKafkaUtils.buildChildSpan("responderUpdateLocation", (IncomingKafkaRecord<String, String>)responderLocationUpdate, tracer);
        return Uni.createFrom().item(responderLocationUpdate).onItem()
                .transform(m -> getLocationUpdate(responderLocationUpdate.getPayload()))
                .onItem().ifNotNull().transformToUni(this::processLocationUpdate)
                .onItem().transform(v -> {
                    span.finish();
                    return responderLocationUpdate.ack();
                });
    }

    private Uni<Void> processLocationUpdate(JsonObject locationUpdate) {
        if (tracer.activeSpan() != null) {
            tracer.activeSpan().setTag("status", locationUpdate.getString("status"));

        }
        Optional<Mission> mission = repository.get(getKey(locationUpdate));
        if (mission.isPresent()) {
            ResponderLocationHistory rlh = new ResponderLocationHistory(BigDecimal.valueOf(locationUpdate.getDouble("lat")),
                    BigDecimal.valueOf(locationUpdate.getDouble("lon")), Instant.now().toEpochMilli());
            mission.get().getResponderLocationHistory().add(rlh);
            return emitMissionEvent(locationUpdate.getString("status"), mission.get())
                    .onItem().transformToUni(m -> emitUpdateResponderCommand(m, locationUpdate))
                    .onItem().transformToUni(m -> repository.add(m));
        } else {
            String stmt = "Mission with key = " + getKey(locationUpdate) + " not found in the repository.";
            log.warn(stmt);
            if (tracer.activeSpan() != null) {
                tracer.activeSpan().setTag("error", "true").log(stmt);
            }
        }
        return Uni.createFrom().item(null);
    }

    private Uni<Mission> emitMissionEvent(String status, Mission mission) {
        if (ResponderLocationStatus.PICKEDUP.name().equals(status)) {
            mission.status(MissionStatus.UPDATED);
            return eventSink.missionPickedUp(mission).map(v -> mission);
        } else if (ResponderLocationStatus.DROPPED.name().equals(status)) {
            mission.status(MissionStatus.COMPLETED);
            return eventSink.missionCompleted(mission).map(v -> mission);
        } else {
            //do nothing
            return Uni.createFrom().item(mission);
        }
    }

    private Uni<Mission> emitUpdateResponderCommand(Mission mission, JsonObject locationUpdate) {
        if (ResponderLocationStatus.DROPPED.name().equals(locationUpdate.getString("status"))) {
            return eventSink.responderCommand(mission, BigDecimal.valueOf(locationUpdate.getDouble("lat")),
                    BigDecimal.valueOf(locationUpdate.getDouble("lon")), locationUpdate.getBoolean("human"))
                    .map(v -> mission);
        } else {
            return Uni.createFrom().item(mission);
        }
    }

    private JsonObject getLocationUpdate(String payload) {
        try {
            JsonObject json = new JsonObject(payload);
            if (json.getString("responderId") == null || json.getString("responderId").isBlank()
                    || json.getString("missionId") == null || json.getString("missionId").isBlank()
                    || json.getString("incidentId") == null || json.getString("incidentId").isBlank()
                    || json.getString("status") == null || json.getString("status").isBlank()
                    || json.getDouble("lat") == null || json.getDouble("lon") == null
                    || json.getBoolean("human") == null || json.getBoolean("continue") == null) {
                log.warn("Unexpected message structure. Message is ignored");
                return null;
            }
            log.debug("Processing message: " + json.toString());
            return json;
        } catch (Exception e) {
            log.warn("Unexpected message structure. Message is ignored");
            return null;
        }
    }

    private String getKey(JsonObject json) {
        return json.getString("incidentId") + ":" + json.getString("responderId");
    }

}
