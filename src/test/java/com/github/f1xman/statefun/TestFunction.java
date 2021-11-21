package com.github.f1xman.statefun;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class TestFunction implements StatefulFunction {

    public static final TypeName TYPE = TypeName.typeNameFromString("com.github.f1xman.statefun/TestFunction");
    public static final String EGRESS_TOPIC = "testcontainers-statefun-out";
    private static final TypeName EGRESS = TypeName.typeNameFromString("testcontainers-statefun/out");

    private final Function<String, String> responseProvider;

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        InOutMessage inOut = message.as(InOutMessage.TYPE);
        String responsePhrase = responseProvider.apply(inOut.value());
        InOutMessage responseMessage = new InOutMessage(responsePhrase);
        EgressMessage egressMessage = KafkaEgressMessage.forEgress(EGRESS)
                .withTopic(EGRESS_TOPIC)
                .withValue(InOutMessage.TYPE, responseMessage)
                .build();
        log.info("Sending message {} to topic {}...", inOut, EGRESS_TOPIC);
        context.send(egressMessage);
        return context.done();
    }

}
