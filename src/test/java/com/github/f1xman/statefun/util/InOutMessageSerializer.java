package com.github.f1xman.statefun.util;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;

import com.github.f1xman.statefun.InOutMessage;

public class InOutMessageSerializer implements Serializer<InOutMessage> {

    @Override
    @SneakyThrows
    public byte[] serialize(String s, InOutMessage o) {
        return JsonMapperProvider.mapper().writeValueAsBytes(o);
    }
}
