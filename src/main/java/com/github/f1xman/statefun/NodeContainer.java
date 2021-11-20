package com.github.f1xman.statefun;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static org.testcontainers.containers.BindMode.READ_ONLY;

@Slf4j
final class NodeContainer extends GenericContainer<NodeContainer> {

    private static final String MODULE_CONTAINER_PATH = "/opt/statefun/modules/remote/module.yaml";
    private static final String FLINK_CONF_CONTAINER_PATH = "/opt/flink/conf/flink-conf.yaml";
    private static final String FLINK_CONF_RESOURCE_PATH = "flink-conf.yaml";

    NodeContainer(DockerImageName dockerImageName, Role role, String moduleResourcePath) {
        super(dockerImageName);
        withClasspathResourceMapping(moduleResourcePath, MODULE_CONTAINER_PATH, READ_ONLY);
        withClasspathResourceMapping(FLINK_CONF_RESOURCE_PATH, FLINK_CONF_CONTAINER_PATH, READ_ONLY);
        withReuse(false);
        role.configureContainer(this);
    }

    enum Role {

        MASTER(6123),
        WORKER(6122);

        private final Integer[] exposedPorts;

        Role(Integer... exposedPorts) {
            this.exposedPorts = exposedPorts;
        }

        public void configureContainer(NodeContainer container) {
            log.info("Configuring container using role {}", this);
            container.withEnv("ROLE", getRoleName());
            container.withEnv("MASTER_HOST", MASTER.getRoleName());
            container.withNetworkAliases(getRoleName());
            container.withExposedPorts(exposedPorts);
        }

        public String getRoleName() {
            return this.name().toLowerCase();
        }
    }
}
