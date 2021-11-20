package com.github.f1xman.statefun;

import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static com.github.f1xman.statefun.ModuleServer.startRemoteModuleServer;
import static com.github.f1xman.statefun.NodeContainer.Role.MASTER;
import static com.github.f1xman.statefun.NodeContainer.Role.WORKER;

public class StatefunContainer implements Startable {

    private final List<NodeContainer> containers;

    public StatefunContainer(DockerImageName dockerImageName, String modulePath) {
        NodeContainer master = new NodeContainer(dockerImageName, MASTER, modulePath);
        NodeContainer worker = new NodeContainer(dockerImageName, WORKER, modulePath).dependsOn(master);
        containers = List.of(master, worker);
    }

    @Override
    public void start() {
        containers.forEach(GenericContainer::start);
    }

    @Override
    public void stop() {
        containers.forEach(GenericContainer::stop);
    }

    public StatefunContainer dependsOn(Startable... startables) {
        containers.forEach(c -> c.dependsOn(startables));
        return this;
    }

    public StatefunContainer withNetwork(Network network) {
        containers.forEach(c -> c.withNetwork(network));
        return this;
    }

    public StatefunContainer withExtraHost(String hostname, String ipAddress) {
        containers.forEach(c -> c.withExtraHost(hostname, ipAddress));
        return this;
    }

    public void startModuleServer(StatefulFunctions statefulFunctions, int port) {
        startRemoteModuleServer(statefulFunctions, port);
    }
}
