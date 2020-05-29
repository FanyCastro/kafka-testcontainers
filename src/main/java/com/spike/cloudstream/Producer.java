package com.spike.cloudstream;

import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;

@EnableBinding(Source.class)
public class Producer {

    private Source mySource;

    public Producer(final Source mySource) {
        this.mySource = mySource;
    }

    public Source getMysource() {
        return mySource;
    }

}
