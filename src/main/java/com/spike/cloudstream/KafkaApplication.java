package com.spike.cloudstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaApplication {
	/*
        Spring Cloud Stream is a binding framework that allows you to bind your piece of code to remotely destination exposed by a broker
        Spring Cloud Stream application types -> Sources (supplier), Sinks (consumer), Processors(function)

		The binder is responsible for communication between the application and the message broker.
		These binders are MessageChannel-based implementations.
		Kafka Streams Binder -> While the contracts established by Spring Cloud Stream are maintained from a programming model perspective,
			Kafka Streams binder does not use MessageChannel as the target type. The binder implementation natively interacts with Kafka Streams “types” - KStream or KTable
 	 */
	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

}
