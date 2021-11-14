package com.github.f1xman.statefun.util;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;

import static com.github.f1xman.statefun.TestFunction.InOutMessage;

public class InOutMessageSerializer implements Serializer<InOutMessage> {

    @Override
    @SneakyThrows
    public byte[] serialize(String s, InOutMessage o) {
        return JsonMapperProvider.mapper().writeValueAsBytes(o);
    }
}
