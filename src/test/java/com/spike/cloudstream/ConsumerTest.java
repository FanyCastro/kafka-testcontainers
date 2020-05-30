package com.spike.cloudstream;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Testcontainers
@ContextConfiguration(initializers = {ConsumerTest.Initializer.class})
public class ConsumerTest {

    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer();

    @Autowired
    private Consumer consumer;
    @Autowired
    private Producer producer;

    private static final int TIMEOUT_WAITING_FOR_MESSAGES = 15;
    private static final String PROPERTY_FORMAT = "spring.cloud.stream.kafka.binder.brokers=%s";

    static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(@NotNull ConfigurableApplicationContext configurableApplicationContext) {
            final String property = String.format(PROPERTY_FORMAT, KAFKA_CONTAINER.getBootstrapServers());
            TestPropertyValues.of(property).applyTo(configurableApplicationContext);
        }
    }

    @Test
    void simpleTestKafkaContainer() {
        assertTrue(KAFKA_CONTAINER.isRunning());
    }

    @Test
    void consumingMessage () {
        final List<String> payloads = Arrays.asList("->one", "->two", "->three", "->four", "->five", "->six");
        final List<Message> messagesToSend = payloads.stream().map(s ->
            MessageBuilder.withPayload(System.currentTimeMillis() + s).build()
        ).collect(Collectors.toList());
        final int totalMessages = messagesToSend.size();

        consumer.resetMessages();
        messagesToSend.forEach(m -> producer.getMysource().output().send(m));

        await().atMost(TIMEOUT_WAITING_FOR_MESSAGES, SECONDS).until(() -> consumer.getMessages().size() == totalMessages);

        assertThat(consumer.getMessages()).hasSize(payloads.size());
    }
}
