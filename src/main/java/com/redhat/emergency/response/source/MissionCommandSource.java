package com.redhat.emergency.response.source;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.emergency.response.map.RoutePlanner;
import com.redhat.emergency.response.model.Mission;
import com.redhat.emergency.response.model.MissionStatus;
import com.redhat.emergency.response.repository.MissionRepository;
import com.redhat.emergency.response.sink.EventSink;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class MissionCommandSource {

    private static final Logger log = LoggerFactory.getLogger(MissionCommandSource.class);

    static final String CREATE_MISSION_COMMAND = "CreateMissionCommand";
    static final String[] ACCEPTED_MESSAGE_TYPES = {CREATE_MISSION_COMMAND};

    @Inject
    RoutePlanner routePlanner;

    @Inject
    MissionRepository repository;

    @Inject
    EventSink eventSink;

    @Incoming("mission-command")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Uni<CompletionStage<Void>> process(Message<String> missionCommandMessage) {

        return Uni.createFrom().item(missionCommandMessage)
                .onItem().transform(mcm -> accept(missionCommandMessage.getPayload()))
                .onItem().transform(o -> o.flatMap(j -> validate(j.getJsonObject("body"))).orElseThrow(() -> new IllegalStateException("Message ignored")))
                .onItem().transform(m -> m.status(MissionStatus.CREATED))
                .onItem().transformToUni(this::addRoute)
                .onItem().transformToUni(this::addToRepositoryAsync)
                .onItem().transformToUni(this::publishMissionStartedEventAsync)
                .onItem().transform(m -> missionCommandMessage.ack())
                .onFailure().recoverWithItem(t -> missionCommandMessage.ack());
    }

    private Uni<Mission> addRoute(Mission mission) {
        return routePlanner.getDirections(mission.responderLocation(), mission.destinationLocation(), mission.incidentLocation())
                .map(missionSteps -> {
                    mission.getSteps().addAll(missionSteps);
                    return mission;
                });
    }

    private Uni<Mission> addToRepositoryAsync(Mission mission) {
        return repository.add(mission).map(v -> mission);
    }

    private Uni<Mission> publishMissionStartedEventAsync(Mission mission) {
        return eventSink.missionStarted(mission).map(v -> mission);
    }

    private Optional<JsonObject> accept(String messageAsJson) {
        try {
            JsonObject json = new JsonObject(messageAsJson);
            String messageType = json.getString("messageType");
            if (Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(messageType) && json.containsKey("body")) {
                log.debug("Processing message: " + json.toString());
                return Optional.of(json);
            }
            log.debug("Message with type '" + messageType + "' is ignored");
        } catch (Exception e) {
            log.warn("Unexpected message which is not JSON or without 'messageType' field.");
            log.warn("Message: " + messageAsJson);
        }
        return Optional.empty();
    }

    private Optional<Mission> validate(JsonObject json) {
        try {
            Optional<Mission> mission = Optional.of(json.mapTo(Mission.class))
                    .filter(m -> m.getIncidentId() != null && !(m.getIncidentId().isBlank()))
                    .filter(m -> m.getResponderId() != null && !(m.getResponderId().isBlank()))
                    .filter(m -> m.getIncidentLat() != null && m.getIncidentLong() != null)
                    .filter(m -> m.getResponderStartLat() != null && m.getResponderStartLong() != null)
                    .filter(m -> m.getDestinationLat() != null && m.getDestinationLong() != null);
            if (mission.isEmpty()) {
                log.warn("Missing data in Mission object. Ignoring.");
            }
            return mission;
        } catch (Exception e) {
            log.error("Exception when deserializing message body into Mission object:", e);
        }
        return Optional.empty();
    }

}
