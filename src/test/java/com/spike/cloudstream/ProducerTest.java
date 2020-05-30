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
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.test.StepVerifier;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void simpleTestKafkaContainer() {
        assertTrue(KAFKA_CONTAINER.isRunning());
    }

    @Test
    @DisplayName("The producer should send a message to the queue")
    void produceSendShouldSendTheMessage() {

        final List<String> messageToSend = Arrays.asList("uno", "dos", "tres");
        messageToSend.forEach(m -> {
            StepVerifier.create(producer.send(Mono.just(m)))
                    .expectSubscription()
                    .thenRequest(Long.MAX_VALUE)
                    .expectNextCount(1L)
                    .expectComplete()
                    .verify();
        });

        final KafkaReceiver<String, String> receiver = getKafkaReceiver();

        // the consumer keeps listening to the queue, so that we have to cancel the subscription
        StepVerifier.create(receiver.receive().map(ConsumerRecord::value))
            .expectSubscription()
            .recordWith(ArrayList::new)  // create an array list with all the elements
            .thenRequest(Long.MAX_VALUE)
            .expectNextCount(messageToSend.size())
            .expectRecordedMatches(messageToSend::containsAll)
            .expectNextCount(0L)
            .thenCancel()
            .verify();
    }

    @NotNull
    private KafkaReceiver<String, String> getKafkaReceiver() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configProperties.getServerUri());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, configProperties.getConsumerClientId());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, configProperties.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final ReceiverOptions<String, String> receiverOptions = ReceiverOptions.create(props);

        final ReceiverOptions<String, String> options = receiverOptions.subscription(
                Collections.singleton(configProperties.getTopic()));
        return KafkaReceiver.create(options);
    }


}
