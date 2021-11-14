package com.github.f1xman.statefun;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.util.concurrent.CompletableFuture;

import static com.github.f1xman.statefun.util.JsonMapperProvider.mapper;

@Slf4j
public class TestFunction implements StatefulFunction {

    public static final TypeName TYPE = TypeName.typeNameFromString("com.github.f1xman.statefun/TestFunction");
    public static final String EGRESS_TOPIC = "testcontainers-statefun-out";
    private static final TypeName EGRESS = TypeName.typeNameFromString("testcontainers-statefun/out");

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        InOutMessage inOut = message.as(InOutMessage.TYPE);
        EgressMessage egressMessage = KafkaEgressMessage.forEgress(EGRESS)
                .withTopic(EGRESS_TOPIC)
                .withValue(InOutMessage.TYPE, inOut)
                .build();
        log.info("Sending message {} to topic {}...", inOut, EGRESS_TOPIC);
        context.send(egressMessage);
        return context.done();
    }

    public record InOutMessage(@JsonProperty("value") String value) {

        public static final Type<InOutMessage> TYPE = SimpleType.simpleImmutableTypeFrom(
                TypeName.typeNameFromString("com.github.f1xman.statefun/in-out-message"),
                mapper()::writeValueAsBytes,
                b -> mapper().readValue(b, InOutMessage.class)
        );

    }
}
