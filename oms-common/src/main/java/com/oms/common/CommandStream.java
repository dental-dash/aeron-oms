package com.oms.common;

import org.agrona.sbe.MessageEncoderFlyweight;

public interface CommandStream {

    // The compiler will infer 'T' based on the variable type assigning it
    <T extends MessageEncoderFlyweight> T encoderOf(Class<T> encoderClass);
    void publish(MessageEncoderFlyweight encoder);
}
