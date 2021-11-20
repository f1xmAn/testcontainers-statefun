package com.github.f1xman.statefun;

import lombok.RequiredArgsConstructor;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static com.github.f1xman.statefun.ModuleServer.startRemoteModuleServer;
import static com.github.f1xman.statefun.StatefunContainer.Role.MASTER;
import static com.github.f1xman.statefun.StatefunContainer.Role.WORKER;

@RequiredArgsConstructor
public class StatefunClusterContainer implements Startable {

    private final List<StatefunContainer> containers;

    public static StatefunClusterContainer create(DockerImageName dockerImageName, String modulePath) {
        StatefunContainer master = new StatefunContainer(dockerImageName, MASTER, modulePath);
        StatefunContainer worker = new StatefunContainer(dockerImageName, WORKER, modulePath).dependsOn(master);
        return new StatefunClusterContainer(List.of(master, worker));
    }

    @Override
    public void start() {
        containers.forEach(GenericContainer::start);
    }

    @Override
    public void stop() {
        containers.forEach(GenericContainer::stop);
    }

    public StatefunClusterContainer dependsOn(Startable... startables) {
        containers.forEach(c -> c.dependsOn(startables));
        return this;
    }

    public StatefunClusterContainer withNetwork(Network network) {
        containers.forEach(c -> c.withNetwork(network));
        return this;
    }

    public StatefunClusterContainer withExtraHost(String hostname, String ipAddress) {
        containers.forEach(c -> c.withExtraHost(hostname, ipAddress));
        return this;
    }

    public void startModuleServer(StatefulFunctions statefulFunctions, int port) {
        startRemoteModuleServer(statefulFunctions, port);
    }
}
