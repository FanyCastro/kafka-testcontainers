package com.spike.cloudstream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.test.StepVerifier;

import java.util.*;

@SpringBootTest
@Testcontainers
public class ProducerTest {
    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer();

    @Autowired
    private Producer producer;
    @Autowired
    private ConfigProperties configProperties;

    private static final String KAFKA_SERVER_URI_PROPERTY = "kafka.properties.server-uri";

    @DynamicPropertySource
    static void KafkaProperties(final DynamicPropertyRegistry registry) {
        registry.add(KAFKA_SERVER_URI_PROPERTY, KAFKA_CONTAINER::getBootstrapServers);
    }

    @Test
    @DisplayName("The producer should send a message to the queue")
    void produceSendShouldSendTheMessage() {
        final List<Pet> petsToSend = Arrays.asList(
                new Pet("Lion"),
                new Pet("Fluffy"),
                new Pet("Bella")
        );
        petsToSend.forEach(pet -> producer.send(Mono.just(pet))
                .as(StepVerifier::create)
                .expectSubscription()
                .thenRequest(Long.MAX_VALUE)
                .expectNextCount(1L)
                .expectComplete()
                .verify());

        // the consumer keeps listening to the queue, so that we have to cancel the subscription
        getPets().as(StepVerifier::create)
                .expectSubscription()
                .recordWith(ArrayList::new)  // create an array list with all the elements
                .thenRequest(Long.MAX_VALUE)
                .expectNextCount(petsToSend.size())
                .expectRecordedMatches(petsToSend::containsAll)
                .expectNextCount(0L)
                .thenCancel()
                .verify();
    }

    private static final String CLIENT_ID = "pet_consumer";
    private static final String GROUP_ID = "pet_consumers";
    private static final String INSTANCE_ID = "pet_consumer_" + UUID.randomUUID().toString();
    private static final String OFFSET_EARLIEST = "earliest";

    private Flux<Pet> getPets() {
        return getKafkaReceiver().receive()
                .map(ConsumerRecord::value);
    }

    @NotNull
    private KafkaReceiver<String, Pet> getKafkaReceiver() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configProperties.getServerUri());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, INSTANCE_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class);
        props.put(GsonDeserializer.CONFIG_VALUE_CLASS, Pet.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_EARLIEST);
        final ReceiverOptions<String, Pet> receiverOptions = ReceiverOptions.create(props);
        final ReceiverOptions<String, Pet> options = receiverOptions
                .subscription(Collections.singleton(configProperties.getTopic()));
        return KafkaReceiver.create(options);
    }
}
