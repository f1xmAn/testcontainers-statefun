package com.github.f1xman.statefun;

import com.github.f1xman.statefun.util.KafkaClient;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static com.github.f1xman.statefun.ModuleServer.getHostAddress;
import static com.github.f1xman.statefun.TestFunction.*;
import static org.testcontainers.shaded.org.awaitility.Awaitility.waitAtMost;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StatefunClusterContainerTest {

    private static final int MODULE_PORT = 8096;
    private static final String INGRESS_TOPIC = "testcontainers-statefun-in";
    private static final String MODULE_HOST = "mbp";
    private static final String FUNCTION_ID = "functionId";

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.1"))
            .withNetwork(Network.SHARED)
            .withNetworkAliases("kafka");
    @Container
    private static final StatefunClusterContainer statefunCluster = StatefunClusterContainer.create(
                    DockerImageName.parse("apache/flink-statefun:3.1.0-java11"),
                    "module.yaml"
            )
            .withNetwork(Network.SHARED)
            .withExtraHost(MODULE_HOST, getHostAddress())
            .dependsOn(kafka);

    private KafkaClient kafkaClient;

    @BeforeAll
    void beforeAll() {
        StatefulFunctions statefulFunctions = new StatefulFunctions()
                .withStatefulFunction(StatefulFunctionSpec.builder(TYPE)
                        .withSupplier(TestFunction::new)
                        .build());
        statefunCluster.startModuleServer(statefulFunctions, MODULE_PORT);
        kafkaClient = KafkaClient.create(kafka.getBootstrapServers());
        kafkaClient.subscribe(EGRESS_TOPIC);
    }

    @Test
    void sendsIngestedMessageToEgress() {
        InOutMessage message = new InOutMessage("Fish and visitors stink after three days");

        kafkaClient.send(INGRESS_TOPIC, FUNCTION_ID, message);

        waitAtMost(Duration.ofMinutes(1)).until(() -> kafkaClient.hasMessage(message));
    }
}