package com.spike.cloudstream;

// When using @EnableBinding(Sink.class) Spring Cloud Stream automatically creates a message channel with the name
// INPUT which is used by the @StreamListener above
//@EnableBinding(Sink.class)
public class Consumer {

//    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
//
//    private ConcurrentLinkedQueue<Message> messages = new ConcurrentLinkedQueue<>();
//
//    public void resetMessages() {
//        messages.clear();
//    }
//
//    public Queue<Message> getMessages() {
//        return new ArrayDeque<>(messages);
//    }
//
//    @StreamListener(target = Sink.INPUT)
//    public void process(final Message<?> message) {
//        messages.add(message);
//        logger.info("Total messages: {}, Received a string message : {}", messages.size(), message);
//    }

}
