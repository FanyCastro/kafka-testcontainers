package com.spike.cloudstream;

import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {

    private Producer producer;
    public Controller(final Producer producer) {
        this.producer = producer;
    }

    // get the String message via HTTP, publish it to broker using spring cloud stream
    @RequestMapping(value = "/sendMessage/string", method = RequestMethod.POST)
    public String publishMessageString(@RequestBody String payload) {
    // send message to channel
        producer.getMysource()
                .output()
                .send(MessageBuilder.withPayload(payload)
                        .setHeader("type", "string")
                        .build());
        return "success";
    }
}
