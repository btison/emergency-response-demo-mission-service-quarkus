package com.redhat.emergency.response.rest;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.emergency.response.repository.MissionRepository;
import io.quarkus.vertx.web.Route;
import io.quarkus.vertx.web.RoutingExchange;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;

@ApplicationScoped
public class RestApi {

    @Inject
    MissionRepository repository;

    @Route(path = "/api/missions", methods = HttpMethod.GET, produces = "application/json")
    void allMissions(RoutingExchange ex) {
        repository.getAll().subscribe().with(missions -> ex.response().putHeader("Content-Type", "application/json")
                .setStatusCode(200).end(Json.encode(missions)));
    }

    @Route(path = "/api/missions/clear", methods = HttpMethod.POST, produces = "application/json")
    void clearAll(RoutingExchange ex) {
        repository.clear().subscribe().with(v -> ex.response().putHeader("Content-Type", "application/json")
                .setStatusCode(204).end());
    }

    @Route(path = "/api/missions/responders/:id", methods = HttpMethod.GET, produces = "application/json")
    void missionByResponder(RoutingExchange ex) {

        ex.getParam("id").ifPresentOrElse(responderId -> repository.getByResponderId(responderId).onItem().transform(l -> l.stream()
                .filter(m -> m.getStatus().equalsIgnoreCase("UPDATED") || (m.getStatus().equalsIgnoreCase("CREATED"))).findFirst())
                .subscribe().with(o -> o.ifPresentOrElse(m -> ex.response().putHeader("Content-Type", "application/json").setStatusCode(200)
                        .end(Json.encode(m)), () -> ex.response().setStatusCode(204).end())), () -> ex.response().setStatusCode(204).end());
    }

}
