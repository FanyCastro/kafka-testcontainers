package com.spike.cloudstream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.HashMap;
import java.util.Map;

@Service
public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class.getName());
    private final KafkaSender<String, String> sender;
    private final ConfigProperties configProperties;
    private final ObjectMapper objectMapper;

    public Producer(final ConfigProperties configProperties, final ObjectMapper objectMapper) {
        this.configProperties = configProperties;
        this.objectMapper = objectMapper;

        final Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configProperties.getServerUri());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, configProperties.getProducerClientId());
        props.put(ProducerConfig.ACKS_CONFIG, configProperties.getAcks());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        SenderOptions<String, String> senderOptions = SenderOptions.create(props);
        sender = KafkaSender.create(senderOptions);
    }

    public Mono<String> send(final Mono<Pet> petMono) {
        return petMono.flatMap(pet -> {
            try {
                final String value = objectMapper.writeValueAsString(pet);
                final SenderRecord<String, String, String> record = SenderRecord.create(
                        new ProducerRecord<>(configProperties.getTopic(), pet.getId(), value), pet.getId()
                );

                log.info("Sending pet {}", pet);
                return sender.send(Flux.just(record)).single().map(ignore -> pet.getId());
            } catch (final JsonProcessingException e) {
                return Mono.error(e);
            }
        });
    }
}
