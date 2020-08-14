package com.redhat.emergency.response.source;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import javax.enterprise.inject.Any;
import javax.inject.Inject;

import com.redhat.emergency.response.map.RoutePlanner;
import com.redhat.emergency.response.model.Location;
import com.redhat.emergency.response.model.Mission;
import com.redhat.emergency.response.model.MissionStatus;
import com.redhat.emergency.response.model.MissionStep;
import com.redhat.emergency.response.repository.MissionRepository;
import com.redhat.emergency.response.sink.EventSink;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.connectors.InMemorySource;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

@QuarkusTest
public class MessageCommandSourceTest {

    @InjectMock
    RoutePlanner routePlanner;

    @InjectMock
    MissionRepository repository;

    @InjectMock
    EventSink eventSink;

    @Inject @Any
    InMemoryConnector connector;

    @Captor
    ArgumentCaptor<Mission> missionCaptor;

    @Captor
    ArgumentCaptor<Location> locationCaptor;

    InMemorySource<Message<String>> source;

    @BeforeEach
    void init() {
        openMocks(this);
        source = connector.source("mission-command");
    }

    @Test
    void testProcessMessage() throws ExecutionException, InterruptedException {

        String payload = "{\"id\":\"91cf5e82-8135-476d-ade4-5fe00dca2cc6\",\"messageType\":\"CreateMissionCommand\","
                + "\"invokingService\":\"IncidentProcessService\",\"timestamp\":1593363522344,\"body\": "
                + "{\"incidentId\":\"incident123\",\"responderId\":\"responder123\",\"responderStartLat\":\"40.12345\","
                + "\"responderStartLong\":\"-80.98765\",\"incidentLat\":\"30.12345\",\"incidentLong\":\"-70.98765\","
                + "\"destinationLat\":\"50.12345\",\"destinationLong\":\"-90.98765\",\"processId\":\"0\"}}";

        MissionStep missionStep1 = new MissionStep();
        MissionStep missionStep2 = new MissionStep();

        when(routePlanner.getDirections(any(Location.class), any(Location.class), any(Location.class)))
                .thenReturn(Uni.createFrom().item(Arrays.asList(missionStep1, missionStep2)));
        when(repository.add(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));
        when(eventSink.missionStarted(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));

        MessageWithAck<String> message = MessageWithAck.of(payload);
        source.send(message);

        assertThat(message.acked(), is(true));
        verify(repository).add(missionCaptor.capture());
        Mission mission = missionCaptor.getValue();
        assertThat(mission, notNullValue());
        assertThat(mission.getIncidentId(), equalTo("incident123"));
        assertThat(mission.getIncidentLat(), equalTo(new BigDecimal("30.12345")));
        assertThat(mission.getIncidentLong(), equalTo(new BigDecimal("-70.98765")));
        assertThat(mission.getResponderId(), equalTo("responder123"));
        assertThat(mission.getResponderStartLat(), equalTo(new BigDecimal("40.12345")));
        assertThat(mission.getResponderStartLong(), equalTo(new BigDecimal("-80.98765")));
        assertThat(mission.getDestinationLat(), equalTo(new BigDecimal("50.12345")));
        assertThat(mission.getDestinationLong(), equalTo(new BigDecimal("-90.98765")));
        assertThat(mission.getSteps().size(), equalTo(2));
        assertThat(mission.getStatus(), equalTo(MissionStatus.CREATED.name()));
        verify(routePlanner).getDirections(locationCaptor.capture(),locationCaptor.capture(), locationCaptor.capture());
        Location location1 = locationCaptor.getAllValues().get(0);
        Location location2 = locationCaptor.getAllValues().get(1);
        Location location3 = locationCaptor.getAllValues().get(2);
        assertThat(location1, notNullValue());
        assertThat(location2, notNullValue());
        assertThat(location3, notNullValue());
        assertThat(location1.getLatitude(), equalTo(new BigDecimal("40.12345")));
        assertThat(location1.getLongitude(), equalTo(new BigDecimal("-80.98765")));
        assertThat(location2.getLatitude(), equalTo(new BigDecimal("50.12345")));
        assertThat(location2.getLongitude(), equalTo(new BigDecimal("-90.98765")));
        assertThat(location3.getLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(location3.getLongitude(), equalTo(new BigDecimal("-70.98765")));
        verify(eventSink).missionStarted(missionCaptor.capture());
        mission = missionCaptor.getValue();
        assertThat(mission, notNullValue());
        assertThat(mission.getIncidentId(), equalTo("incident123"));
        assertThat(mission.getIncidentLat(), equalTo(new BigDecimal("30.12345")));
        assertThat(mission.getIncidentLong(), equalTo(new BigDecimal("-70.98765")));
        assertThat(mission.getResponderId(), equalTo("responder123"));
        assertThat(mission.getResponderStartLat(), equalTo(new BigDecimal("40.12345")));
        assertThat(mission.getResponderStartLong(), equalTo(new BigDecimal("-80.98765")));
        assertThat(mission.getDestinationLat(), equalTo(new BigDecimal("50.12345")));
        assertThat(mission.getDestinationLong(), equalTo(new BigDecimal("-90.98765")));
        assertThat(mission.getSteps().size(), equalTo(2));
        assertThat(mission.getStatus(), equalTo(MissionStatus.CREATED.name()));
    }

    @Test
    void testProcessMessageBadMessageType() throws ExecutionException, InterruptedException {

        String payload = "{\"id\":\"91cf5e82-8135-476d-ade4-5fe00dca2cc6\",\"messageType\":\"WrongMessageType\","
                + "\"invokingService\":\"IncidentProcessService\",\"timestamp\":1593363522344,\"body\": "
                + "{\"incidentId\":\"incident123\",\"responderId\":\"responder123\",\"responderStartLat\":\"40.12345\","
                + "\"responderStartLong\":\"-80.98765\",\"incidentLat\":\"30.12345\",\"incidentLong\":\"-70.98765\","
                + "\"destinationLat\":\"50.12345\",\"destinationLong\":\"-90.98765\",\"processId\":\"0\"}}";

        MessageWithAck<String> message = MessageWithAck.of(payload);
        source.send(message);

        assertThat(message.acked(), is(true));
        verify(repository, never()).add(any(Mission.class));
        verify(routePlanner, never()).getDirections(any(Location.class), any(Location.class), any(Location.class));
        verify(eventSink, never()).missionStarted(any(Mission.class));
    }

    @Test
    void testProcessMessageMissingFields() throws ExecutionException, InterruptedException {

        String payload = "{\"id\":\"91cf5e82-8135-476d-ade4-5fe00dca2cc6\",\"messageType\":\"WrongMessageType\","
                + "\"invokingService\":\"IncidentProcessService\",\"timestamp\":1593363522344,\"body\": "
                + "{\"incidentId\":\"incident123\",\"responderId\":\"responder123\","
                + "\"incidentLat\":\"30.12345\",\"incidentLong\":\"-70.98765\","
                + "\"destinationLat\":\"50.12345\",\"destinationLong\":\"-90.98765\",\"processId\":\"0\"}}";

        MessageWithAck<String> message = MessageWithAck.of(payload);
        source.send(message);

        assertThat(message.acked(), is(true));
        verify(repository, never()).add(any(Mission.class));
        verify(routePlanner, never()).getDirections(any(Location.class), any(Location.class), any(Location.class));
        verify(eventSink, never()).missionStarted(any(Mission.class));
    }
}
