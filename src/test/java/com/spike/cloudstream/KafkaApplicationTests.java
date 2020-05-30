package com.spike.cloudstream;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.test.StepVerifier;

@SpringBootTest
class KafkaApplicationTests {

	@Autowired
	private Producer producer;

	@Test
	void contextLoads() {
		StepVerifier.create(producer.send("hola"))
				.expectSubscription()
				.thenRequest(Long.MAX_VALUE)
				.expectNextCount(1L)
				.expectComplete()
				.verify();
	}

}
