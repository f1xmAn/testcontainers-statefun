package com.github.f1xman.statefun;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import static com.github.f1xman.statefun.util.JsonMapperProvider.mapper;

public record InOutMessage(@JsonProperty("value") String value) {

    public static final Type<InOutMessage> TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.github.f1xman.statefun/in-out-message"),
            mapper()::writeValueAsBytes,
            b -> mapper().readValue(b, InOutMessage.class)
    );

}
