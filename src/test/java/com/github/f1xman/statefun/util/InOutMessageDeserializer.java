package com.github.f1xman.statefun.util;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Deserializer;

import static com.github.f1xman.statefun.TestFunction.InOutMessage;

public class InOutMessageDeserializer implements Deserializer<InOutMessage> {

    @Override
    @SneakyThrows
    public InOutMessage deserialize(String s, byte[] bytes) {
        return JsonMapperProvider.mapper().readValue(bytes, InOutMessage.class);
    }
}
