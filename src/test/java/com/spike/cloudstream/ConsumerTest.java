package com.spike.cloudstream;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Testcontainers
@ContextConfiguration(initializers = {ConsumerTest.Initializer.class})
public class ConsumerTest {

    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer();

//    @Autowired
//    private Consumer consumer;
    @Autowired
    private Producer producer;

    private static final int TIMEOUT_WAITING_FOR_MESSAGES = 15;
    private static final String PROPERTY_FORMAT = "spring.cloud.stream.kafka.binder.brokers=%s";

    static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
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
        producer.send("test");
    }
}
