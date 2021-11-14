package com.github.f1xman.statefun.util;

import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.NoArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class JsonMapperProvider {

    private static final JsonMapper mapper = JsonMapper.builder().build();

    public static JsonMapper mapper() {
        return mapper;
    }
}
