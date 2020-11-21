package com.redhat.emergency.response.repository;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodeAbsent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodePresent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import com.redhat.emergency.response.model.Mission;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCacheManagerAdmin;
import org.infinispan.commons.util.CloseableIteratorSetAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

public class MissionRepositoryTest {

    MissionRepository repository;

    @Mock
    RemoteCacheManager cacheManager;

    @Mock
    RemoteCache<String, String> missionCache;

    @Mock
    RemoteCacheManagerAdmin cacheManagerAdmin;

    @Captor
    ArgumentCaptor<String> stringCaptor;

    @BeforeEach
    void init() {
        openMocks(this);
        repository = new MissionRepository();
        setField(repository, "cacheName", "mission");
        setField(repository, "cacheManager", cacheManager);
        when(cacheManager.administration()).thenReturn(cacheManagerAdmin);
        when(cacheManagerAdmin.<String, String>getOrCreateCache(eq("mission"), any(Configuration.class))).thenReturn(missionCache);
    }

    @Test
    void testAdd() {

        JsonObject json = new JsonObject().put("incidentId", "incident123")
                .put("incidentLat", new BigDecimal("30.12345").doubleValue()).put("incidentLong", new BigDecimal("-70.98765").doubleValue())
                .put("responderId", "responder123")
                .put("responderStartLat", new BigDecimal("31.12345").doubleValue()).put("responderStartLong", new BigDecimal("-71.98765").doubleValue())
                .put("destinationLat", new BigDecimal("32.12345").doubleValue()).put("destinationLong", new BigDecimal("-72.98765").doubleValue())
                .put("status", "CREATED");

        Mission mission = json.mapTo(Mission.class);

        repository.add(mission).await().indefinitely();

        verify(missionCache).put(eq(mission.getKey()), stringCaptor.capture());
        String value = stringCaptor.getValue();
        assertThat(value, notNullValue());
        assertThat(value, jsonNodePresent("id"));
        assertThat(value, jsonPartEquals("id", "${json-unit.regex}[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"));
        assertThat(value, jsonPartEquals("responderId", "responder123"));
        assertThat(value, jsonPartEquals("incidentLat", 30.12345));
        assertThat(value, jsonPartEquals("incidentLong", -70.98765));
        assertThat(value, jsonPartEquals("responderStartLat", 31.12345));
        assertThat(value, jsonPartEquals("responderStartLong", -71.98765));
        assertThat(value, jsonPartEquals("destinationLat", 32.12345));
        assertThat(value, jsonPartEquals("destinationLong", -72.98765));
        assertThat(value, jsonNodePresent("steps"));
        assertThat(value, jsonNodeAbsent("steps[0]"));
        assertThat(value, jsonNodePresent("responderLocationHistory"));
        assertThat(value, jsonNodeAbsent("responderLocationHistory[0]"));
        assertThat(value, jsonPartEquals("status", "CREATED"));
    }

    @Test
    void testGet() {

        JsonObject json = new JsonObject().put("incidentId", "incident123")
                .put("incidentLat", new BigDecimal("30.12345").doubleValue()).put("incidentLong", new BigDecimal("-70.98765").doubleValue())
                .put("responderId", "responder123")
                .put("responderStartLat", new BigDecimal("31.12345").doubleValue()).put("responderStartLong", new BigDecimal("-71.98765").doubleValue())
                .put("destinationLat", new BigDecimal("32.12345").doubleValue()).put("destinationLong", new BigDecimal("-72.98765").doubleValue())
                .put("status", "CREATED")
                .put("responderLocationHistory", new JsonArray().add(new JsonObject().put("lat", new BigDecimal("30.98765").doubleValue())
                        .put("lon", new BigDecimal("-70.12345").doubleValue()).put("timestamp", 12345L)))
                .put("steps", new JsonArray().add(new JsonObject().put("lat", new BigDecimal("30.98765").doubleValue())
                        .put("lon", new BigDecimal("-70.12345").doubleValue()).put("wayPoint", false).put("destination", false)));

        when(missionCache.get("key")).thenReturn(json.toString());

        Optional<Mission> mission = repository.get("key").await().indefinitely();
        assertThat(mission, notNullValue());
        assertThat(mission.isPresent(), equalTo(true));
        assertThat(mission.get().getIncidentId(), equalTo("incident123"));
        assertThat(mission.get().getResponderId(), equalTo("responder123"));
        assertThat(mission.get().getIncidentLat(), equalTo(new BigDecimal("30.12345")));
        assertThat(mission.get().getIncidentLong(), equalTo(new BigDecimal("-70.98765")));
        assertThat(mission.get().getResponderStartLat(), equalTo(new BigDecimal("31.12345")));
        assertThat(mission.get().getResponderStartLong(), equalTo(new BigDecimal("-71.98765")));
        assertThat(mission.get().getDestinationLat(), equalTo(new BigDecimal("32.12345")));
        assertThat(mission.get().getDestinationLong(), equalTo(new BigDecimal("-72.98765")));
        assertThat(mission.get().getStatus(), equalTo("CREATED"));
        assertThat(mission.get().getResponderLocationHistory().size(), equalTo(1));
        assertThat(mission.get().getSteps().size(), equalTo(1));
        verify(missionCache).get("key");
    }

    @Test
    void testGetNotFound() {

        when(missionCache.get("key")).thenReturn(null);

        Optional<Mission> mission = repository.get("key").await().indefinitely();
        assertThat(mission, notNullValue());
        assertThat(mission.isPresent(), is(false));
        verify(missionCache).get("key");
    }

    @Test
    void testGetThrowsException() {

        when(missionCache.get("key")).thenThrow(new RuntimeException("Exception!"));

        Optional<Mission> mission = repository.get("key").await().indefinitely();
        assertThat(mission, notNullValue());
        assertThat(mission.isPresent(), is(false));
        verify(missionCache).get("key");
    }

    @Test
    void testGetAll() {

        JsonObject json1 = new JsonObject().put("incidentId", "incident123")
                .put("incidentLat", new BigDecimal("30.12345").doubleValue()).put("incidentLong", new BigDecimal("-70.98765").doubleValue())
                .put("responderId", "responder123")
                .put("responderStartLat", new BigDecimal("31.12345").doubleValue()).put("responderStartLong", new BigDecimal("-71.98765").doubleValue())
                .put("destinationLat", new BigDecimal("32.12345").doubleValue()).put("destinationLong", new BigDecimal("-72.98765").doubleValue())
                .put("status", "CREATED")
                .put("responderLocationHistory", new JsonArray().add(new JsonObject().put("lat", new BigDecimal("30.98765").doubleValue())
                        .put("lon", new BigDecimal("-70.12345").doubleValue()).put("timestamp", 12345L)))
                .put("steps", new JsonArray().add(new JsonObject().put("lat", new BigDecimal("30.98765").doubleValue())
                        .put("lon", new BigDecimal("-70.12345").doubleValue()).put("wayPoint", false).put("destination", false)));

        JsonObject json2 = new JsonObject().put("incidentId", "incident456")
                .put("incidentLat", new BigDecimal("30.12345").doubleValue()).put("incidentLong", new BigDecimal("-70.98765").doubleValue())
                .put("responderId", "responder456")
                .put("responderStartLat", new BigDecimal("31.12345").doubleValue()).put("responderStartLong", new BigDecimal("-71.98765").doubleValue())
                .put("destinationLat", new BigDecimal("32.12345").doubleValue()).put("destinationLong", new BigDecimal("-72.98765").doubleValue())
                .put("status", "CREATED")
                .put("responderLocationHistory", new JsonArray().add(new JsonObject().put("lat", new BigDecimal("30.98765").doubleValue())
                        .put("lon", new BigDecimal("-70.12345").doubleValue()).put("timestamp", 12345L)))
                .put("steps", new JsonArray().add(new JsonObject().put("lat", new BigDecimal("30.98765").doubleValue())
                        .put("lon", new BigDecimal("-70.12345").doubleValue()).put("wayPoint", false).put("destination", false)));

        when(missionCache.keySet()).thenReturn(new CloseableIteratorSetAdapter<>(new HashSet<>(Arrays.asList("incident123:responder123", "incident456:responder456"))));
        when(missionCache.get("incident123:responder123")).thenReturn(json1.toString());
        when(missionCache.get("incident456:responder456")).thenReturn(json2.toString());

        List<Mission> missions = repository.getAll().await().indefinitely();

        assertThat(missions, notNullValue());
        assertThat(missions.size(), equalTo(2));
        assertThat(missions.get(0).getIncidentId(), anyOf(equalTo("incident123"), equalTo("incident456")));
        verify(missionCache).keySet();
        verify(missionCache).get("incident123:responder123");
        verify(missionCache).get("incident456:responder456");
    }

    @Test
    void testGetAllCacheEmpty() {

        when(missionCache.keySet()).thenReturn(new CloseableIteratorSetAdapter<>(Collections.emptySet()));

        List<Mission> missions = repository.getAll().await().indefinitely();

        assertThat(missions, notNullValue());
        assertThat(missions.size(), equalTo(0));
    }

    @Test
    void clear() {

        repository.clear().await().indefinitely();

        verify(missionCache).clear();
    }

    @Test
    void testGetByResponderId() {
        JsonObject json1 = new JsonObject().put("incidentId", "incident123")
                .put("incidentLat", new BigDecimal("30.12345").doubleValue()).put("incidentLong", new BigDecimal("-70.98765").doubleValue())
                .put("responderId", "responder123")
                .put("responderStartLat", new BigDecimal("31.12345").doubleValue()).put("responderStartLong", new BigDecimal("-71.98765").doubleValue())
                .put("destinationLat", new BigDecimal("32.12345").doubleValue()).put("destinationLong", new BigDecimal("-72.98765").doubleValue())
                .put("status", "CREATED")
                .put("responderLocationHistory", new JsonArray().add(new JsonObject().put("lat", new BigDecimal("30.98765").doubleValue())
                        .put("lon", new BigDecimal("-70.12345").doubleValue()).put("timestamp", 12345L)))
                .put("steps", new JsonArray().add(new JsonObject().put("lat", new BigDecimal("30.98765").doubleValue())
                        .put("lon", new BigDecimal("-70.12345").doubleValue()).put("wayPoint", false).put("destination", false)));

        JsonObject json2 = new JsonObject().put("incidentId", "incident456")
                .put("incidentLat", new BigDecimal("30.12345").doubleValue()).put("incidentLong", new BigDecimal("-70.98765").doubleValue())
                .put("responderId", "responder456")
                .put("responderStartLat", new BigDecimal("31.12345").doubleValue()).put("responderStartLong", new BigDecimal("-71.98765").doubleValue())
                .put("destinationLat", new BigDecimal("32.12345").doubleValue()).put("destinationLong", new BigDecimal("-72.98765").doubleValue())
                .put("status", "CREATED")
                .put("responderLocationHistory", new JsonArray().add(new JsonObject().put("lat", new BigDecimal("30.98765").doubleValue())
                        .put("lon", new BigDecimal("-70.12345").doubleValue()).put("timestamp", 12345L)))
                .put("steps", new JsonArray().add(new JsonObject().put("lat", new BigDecimal("30.98765").doubleValue())
                        .put("lon", new BigDecimal("-70.12345").doubleValue()).put("wayPoint", false).put("destination", false)));

        when(missionCache.keySet()).thenReturn(new CloseableIteratorSetAdapter<>(new HashSet<>(Arrays.asList("incident123:responder123", "incident456:responder456"))));
        when(missionCache.get("incident123:responder123")).thenReturn(json1.toString());
        when(missionCache.get("incident456:responder456")).thenReturn(json2.toString());

        List<Mission> missions =  repository.getByResponderId("responder456").await().indefinitely();

        assertThat(missions, notNullValue());
        assertThat(missions.size(), equalTo(1));
        assertThat(missions.get(0).getIncidentId(), equalTo("incident456"));
        verify(missionCache).keySet();
        verify(missionCache).get("incident123:responder123");
        verify(missionCache).get("incident456:responder456");
    }

    private void setField(Object targetObject, String name, Object value) {

        Class<?> targetClass = targetObject.getClass();
        Field field = findField(targetClass, name);
        if (field == null) {
            throw new IllegalArgumentException(String.format(
                    "Could not find field '%s' on %s", name, targetClass));
        }

        makeAccessible(field, targetObject);
        setField(field, targetObject, value);
    }

    private Field findField(Class<?> clazz, String name) {
        Class<?> searchType = clazz;
        while (Object.class != searchType && searchType != null) {
            Field[] fields = getDeclaredFields(searchType);
            for (Field field : fields) {
                if (name.equals(field.getName())) {
                    return field;
                }
            }
            searchType = searchType.getSuperclass();
        }
        return null;
    }

    private Field[] getDeclaredFields(Class<?> clazz) {
        try {
            return clazz.getDeclaredFields();
        }
        catch (Throwable ex) {
            throw new IllegalStateException("Failed to introspect Class [" + clazz.getName() +
                    "] from ClassLoader [" + clazz.getClassLoader() + "]", ex);
        }
    }

    private void makeAccessible(Field field, Object object) {
        if ((!Modifier.isPublic(field.getModifiers()) ||
                !Modifier.isPublic(field.getDeclaringClass().getModifiers()) ||
                Modifier.isFinal(field.getModifiers())) && !field.canAccess(object)) {
            field.setAccessible(true);
        }
    }

    private void setField(Field field, Object target, Object value) {
        try {
            field.set(target, value);
        }
        catch (IllegalAccessException ex) {
            throw new IllegalStateException(
                    "Unexpected reflection exception - " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

}
