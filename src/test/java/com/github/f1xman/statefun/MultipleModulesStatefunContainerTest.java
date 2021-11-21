package com.github.f1xman.statefun;

import com.github.f1xman.statefun.util.KafkaClient;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.function.Function;

import static com.github.f1xman.statefun.ModuleServer.getHostAddress;
import static com.github.f1xman.statefun.TestFunction.EGRESS_TOPIC;
import static com.github.f1xman.statefun.TestFunction.TYPE;
import static org.testcontainers.containers.GenericContainer.INTERNAL_HOST_HOSTNAME;
import static org.testcontainers.shaded.org.awaitility.Awaitility.waitAtMost;

@Testcontainers
class MultipleModulesStatefunContainerTest {

    private static final int MODULE_PORT = 8096;
    private static final String INGRESS_TOPIC = "testcontainers-statefun-in";
    private static final String FUNCTION_ID = "functionId";

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.1"))
            .withNetwork(Network.SHARED)
            .withNetworkAliases("kafka");
    @Container
    private static final StatefunContainer statefun = new StatefunContainer(
            DockerImageName.parse("apache/flink-statefun:3.1.0-java11"),
            "module.yaml"
    )
            .withNetwork(Network.SHARED)
            .withExtraHost(INTERNAL_HOST_HOSTNAME, getHostAddress())
            .dependsOn(kafka);

    private static KafkaClient kafkaClient;

    @BeforeAll
    static void beforeAll() {
        kafkaClient = KafkaClient.create(kafka.getBootstrapServers());
        kafkaClient.subscribe(EGRESS_TOPIC);
    }

    @Test
    void sendsIngestedMessageToEgress() {
        StatefulFunctions statefulFunctions = new StatefulFunctions()
                .withStatefulFunction(StatefulFunctionSpec.builder(TYPE)
                        .withSupplier(() -> new TestFunction(Function.identity()))
                        .build());
        statefun.deployStatefulFunctions(statefulFunctions, MODULE_PORT);
        InOutMessage message = new InOutMessage("Fish and visitors stink after three days");

        kafkaClient.send(INGRESS_TOPIC, FUNCTION_ID, message);

        waitAtMost(Duration.ofMinutes(1)).until(() -> kafkaClient.hasMessage(message));
    }

    @Test
    void sendsConstantMessageToEgress() {
        String expectedText = "Doesn’t expecting the unexpected make the unexpected expected?";
        InOutMessage expected = new InOutMessage(expectedText);
        StatefulFunctions statefulFunctions = new StatefulFunctions()
                .withStatefulFunction(StatefulFunctionSpec.builder(TYPE)
                        .withSupplier(() -> new TestFunction(p -> expectedText))
                        .build());
        statefun.deployStatefulFunctions(statefulFunctions, MODULE_PORT);
        InOutMessage message = new InOutMessage("Fish and visitors stink after three days");

        kafkaClient.send(INGRESS_TOPIC, FUNCTION_ID, message);

        waitAtMost(Duration.ofMinutes(1)).until(() -> kafkaClient.hasMessage(expected));
    }

}