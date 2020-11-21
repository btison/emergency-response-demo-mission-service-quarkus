package com.redhat.emergency.response.rest;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodeAbsent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodePresent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartMatches;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import com.redhat.emergency.response.model.Mission;
import com.redhat.emergency.response.repository.MissionRepository;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.restassured.RestAssured;
import io.restassured.http.Header;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.Json;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class RestApiTest {

    @InjectMock
    MissionRepository repository;

    @Test
    void testGetAll() {

        String m1 = "{\"id\":\"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\"incidentId\":\"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\"," +
                "\"responderId\":\"64\",\"responderStartLat\":\"40.12345\",\"responderStartLong\":\"-80.98765\"," +
                "\"incidentLat\":\"30.12345\",\"incidentLong\":\"-70.98765\"," +
                "\"destinationLat\":\"50.12345\",\"destinationLong\":\"-90.98765\"," +
                "\"responderLocationHistory\":[{\"lat\":30.78452,\"lon\":-70.85252,\"timestamp\":1593872667576}]," +
                "\"steps\":[],\"status\":\"CREATED\"}";
        Mission mission1 = Json.decodeValue(m1, Mission.class);

        String m2 = "{\"id\":\"f5a9bc5e-408c-4f86-8592-6f67bb73c5ff\",\"incidentId\":\"5d9b2d3a-136f-414f-96ba-1b2a445fee5f\"," +
                "\"responderId\":\"68\",\"responderStartLat\":\"40.12345\",\"responderStartLong\":\"-80.98765\"," +
                "\"incidentLat\":\"30.12345\",\"incidentLong\":\"-70.98765\"," +
                "\"destinationLat\":\"50.12345\",\"destinationLong\":\"-90.98765\"," +
                "\"responderLocationHistory\":[{\"lat\":30.78452,\"lon\":-70.85252,\"timestamp\":1593872667576}]," +
                "\"steps\":[],\"status\":\"CREATED\"}";
        Mission mission2 = Json.decodeValue(m2, Mission.class);

        when(repository.getAll()).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(Arrays.asList(mission1, mission2))));

        String response = RestAssured.get("/api/missions").then()
                .assertThat()
                .statusCode(200)
                .contentType("application/json")
                .extract()
                .asString();

        assertThat(response, notNullValue());
        assertThat(response, jsonNodePresent("[0]"));
        assertThat(response, jsonNodePresent("[1]"));
        assertThat(response, jsonNodeAbsent("[2]"));
        assertThat(response, jsonPartMatches("[0].id", anyOf(equalTo("f5a9bc5e-408c-4f86-8592-6f67bb73c5fd"), equalTo("f5a9bc5e-408c-4f86-8592-6f67bb73c5ff"))));
        verify(repository).getAll();
    }

    @Test
    void testClear() {

        when(repository.clear()).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));

        RestAssured.given().header(new Header("Accept", "application/json")).post("/api/missions/clear").then()
                .assertThat()
                .statusCode(204);

        verify(repository).clear();

    }

    @Test
    void missionByResponder() {

        String m1 = "{\"id\":\"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\"incidentId\":\"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\"," +
                "\"responderId\":\"64\",\"responderStartLat\":\"40.12345\",\"responderStartLong\":\"-80.98765\"," +
                "\"incidentLat\":\"30.12345\",\"incidentLong\":\"-70.98765\"," +
                "\"destinationLat\":\"50.12345\",\"destinationLong\":\"-90.98765\"," +
                "\"responderLocationHistory\":[{\"lat\":30.78452,\"lon\":-70.85252,\"timestamp\":1593872667576}]," +
                "\"steps\":[],\"status\":\"COMPLETED\"}";
        Mission mission1 = Json.decodeValue(m1, Mission.class);

        String m2 = "{\"id\":\"f5a9bc5e-408c-4f86-8592-6f67bb73c5ff\",\"incidentId\":\"5d9b2d3a-136f-414f-96ba-1b2a445fee5f\"," +
                "\"responderId\":\"64\",\"responderStartLat\":\"40.12345\",\"responderStartLong\":\"-80.98765\"," +
                "\"incidentLat\":\"30.12345\",\"incidentLong\":\"-70.98765\"," +
                "\"destinationLat\":\"50.12345\",\"destinationLong\":\"-90.98765\"," +
                "\"responderLocationHistory\":[{\"lat\":30.78452,\"lon\":-70.85252,\"timestamp\":1593872667576}]," +
                "\"steps\":[],\"status\":\"CREATED\"}";
        Mission mission2 = Json.decodeValue(m2, Mission.class);

        when(repository.getByResponderId("64")).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(Arrays.asList(mission1, mission2))));

        RestAssured.given().header(new Header("Accept", "application/json")).get("/api/missions/responders/64").then()
                .assertThat()
                .statusCode(200)
                .contentType("application/json")
                .body("id", equalTo("f5a9bc5e-408c-4f86-8592-6f67bb73c5ff"));

        verify(repository).getByResponderId("64");
    }

    @Test
    void missionByResponderNoCurrentMission() {

        String m1 = "{\"id\":\"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\"incidentId\":\"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\"," +
                "\"responderId\":\"64\",\"responderStartLat\":\"40.12345\",\"responderStartLong\":\"-80.98765\"," +
                "\"incidentLat\":\"30.12345\",\"incidentLong\":\"-70.98765\"," +
                "\"destinationLat\":\"50.12345\",\"destinationLong\":\"-90.98765\"," +
                "\"responderLocationHistory\":[{\"lat\":30.78452,\"lon\":-70.85252,\"timestamp\":1593872667576}]," +
                "\"steps\":[],\"status\":\"COMPLETED\"}";
        Mission mission1 = Json.decodeValue(m1, Mission.class);

        String m2 = "{\"id\":\"f5a9bc5e-408c-4f86-8592-6f67bb73c5ff\",\"incidentId\":\"5d9b2d3a-136f-414f-96ba-1b2a445fee5f\"," +
                "\"responderId\":\"64\",\"responderStartLat\":\"40.12345\",\"responderStartLong\":\"-80.98765\"," +
                "\"incidentLat\":\"30.12345\",\"incidentLong\":\"-70.98765\"," +
                "\"destinationLat\":\"50.12345\",\"destinationLong\":\"-90.98765\"," +
                "\"responderLocationHistory\":[{\"lat\":30.78452,\"lon\":-70.85252,\"timestamp\":1593872667576}]," +
                "\"steps\":[],\"status\":\"COMPLETED\"}";
        Mission mission2 = Json.decodeValue(m2, Mission.class);

        when(repository.getByResponderId("64")).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(Arrays.asList(mission1, mission2))));

        RestAssured.given().header(new Header("Accept", "application/json")).get("/api/missions/responders/64").then()
                .assertThat()
                .statusCode(204);

        verify(repository).getByResponderId("64");
    }

    @Test
    void missionByResponderNoMission() {

        when(repository.getByResponderId("64")).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(Collections.emptyList())));

        RestAssured.given().header(new Header("Accept", "application/json")).get("/api/missions/responders/64").then()
                .assertThat()
                .statusCode(204);

        verify(repository).getByResponderId("64");
    }
}
