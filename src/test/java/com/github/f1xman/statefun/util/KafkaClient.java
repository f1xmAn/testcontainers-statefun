package com.github.f1xman.statefun.util;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.github.f1xman.statefun.InOutMessage;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaClient {

    private static final Duration timeout = Duration.ofMinutes(1);
    private final KafkaProducer<String, InOutMessage> producer;
    private final KafkaConsumer<String, InOutMessage> consumer;
    private final Set<InOutMessage> incomingMessages = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public static KafkaClient create(String bootstrapServers) {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("acks", "all");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "com.github.f1xman.statefun.util.InOutMessageSerializer");

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", bootstrapServers);
        consumerProps.setProperty("group.id", "test");
        consumerProps.setProperty("enable.auto.commit", "true");
        consumerProps.setProperty("auto.commit.interval.ms", "1000");
        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "com.github.f1xman.statefun.util.InOutMessageDeserializer");
        return new KafkaClient(new KafkaProducer<>(producerProps), new KafkaConsumer<>(consumerProps));
    }

    @SneakyThrows
    public void send(String topic, String key, InOutMessage message) {
        producer.send(new ProducerRecord<>(topic, key, message)).get(timeout.getSeconds(), TimeUnit.SECONDS);
    }

    public void subscribe(String topic) {
        consumer.subscribe(List.of(topic));
        Executors.newSingleThreadExecutor().execute(() -> {
            while (!Thread.interrupted()) {
                ConsumerRecords<String, InOutMessage> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(r -> incomingMessages.add(r.value()));
            }
        });
    }

    public boolean hasMessage(InOutMessage message) {
        return incomingMessages.contains(message);
    }

}
