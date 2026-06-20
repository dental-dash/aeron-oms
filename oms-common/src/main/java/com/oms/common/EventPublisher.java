package com.oms.common;

import org.agrona.sbe.MessageEncoderFlyweight;

public interface EventPublisher {

    void publish(MessageEncoderFlyweight encoder);
}
