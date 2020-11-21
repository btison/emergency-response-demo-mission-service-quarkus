package com.redhat.emergency.response.repository;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import com.redhat.emergency.response.model.Mission;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class MissionRepository {

    private static final Logger log = LoggerFactory.getLogger(MissionRepository.class);

    @ConfigProperty(name = "infinispan.cache.name.mission", defaultValue = "mission")
    String cacheName;

    @ConfigProperty(name = "infinispan.cache.create.lazy", defaultValue = "false")
    boolean lazy;

    @Inject
    RemoteCacheManager cacheManager;

    volatile RemoteCache<String, String> missionCache;

    void onStart(@Observes StartupEvent e) {
        // do not initialize the cache at startup when remote cache is not available, e.g. in QuarkusTests
        if (!lazy) {
            log.info("Creating remote cache");
            missionCache = initCache();
        }
    }

    // todo: error handling
    public Uni<Void> add(Mission mission) {

       return Uni.createFrom().<Void>item(() -> {
            getCache().put(mission.getKey(), mission.toJson());
            return null;
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    public Uni<Optional<Mission>> get(String key) {

        return Uni.createFrom().<Optional<Mission>>item(() -> {
            try {
                String s = getCache().get(key);
                if (s == null) {
                    return Optional.empty();
                } else {
                    Mission mission = Json.decodeValue(s, Mission.class);
                    return Optional.of(mission);
                }
            } catch (Exception e) {
                log.error("Error when retrieving mission with id '" + key + "'.", e);
                return Optional.empty();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    public Uni<List<Mission>> getAll() {
        return Uni.createFrom().<List<Mission>>item(() -> {
            return getCache().keySet().stream().map(key -> {
                Mission mission = null;
                try {
                    mission = Json.decodeValue(getCache().get(key), Mission.class);
                } catch (DecodeException e) {
                    log.error("Exception decoding mission with id = " + key, e);
                }
                return mission;
            }).filter(Objects::nonNull).collect(Collectors.toList());
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    public Uni<Void> clear() {

        return Uni.createFrom().<Void>item(() -> {
            getCache().clear();
            return null;
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());

    }

    public Uni<List<Mission>> getByResponderId(String responderId) {

        return Uni.createFrom().item(() -> getCache().keySet().stream().map(key -> {
            Mission mission = null;
            try {
                mission = Json.decodeValue(getCache().get(key), Mission.class);
            } catch (DecodeException e) {
                log.error("Exception decoding mission with id = " + key, e);
            }
            return mission;
        }).filter(Objects::nonNull).filter(m -> m.getResponderId().equals(responderId)).collect(Collectors.toList()))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private RemoteCache<String, String> getCache() {
        RemoteCache<String, String> cache = missionCache;
        if (cache == null) {
            synchronized(this) {
                if (missionCache == null) {
                    missionCache = cache = initCache();
                }
            }
        }
        return cache;
    }

    private RemoteCache<String, String> initCache() {
        Configuration configuration = Configuration.builder().name("mission").mode("SYNC").owners(2).build();
        return cacheManager.administration().getOrCreateCache(cacheName, configuration);
    }

}
