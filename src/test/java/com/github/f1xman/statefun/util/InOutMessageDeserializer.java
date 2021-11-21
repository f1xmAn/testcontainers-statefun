package com.github.f1xman.statefun.util;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Deserializer;

import com.github.f1xman.statefun.InOutMessage;

public class InOutMessageDeserializer implements Deserializer<InOutMessage> {

    @Override
    @SneakyThrows
    public InOutMessage deserialize(String s, byte[] bytes) {
        return JsonMapperProvider.mapper().readValue(bytes, InOutMessage.class);
    }
}
