package com.lisz.serialize;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

public class ObjectSerializer implements Serializer {

    @Override
    public byte[] serialize(String topic, Object data) {
        return SerializationUtils.serialize((Serializable) data);
    }

    @Override
    public void close() {
        System.out.println("close");
    }

    @Override
    public void configure(Map configs, boolean isKey) {
        System.out.println("configure");
    }
}
