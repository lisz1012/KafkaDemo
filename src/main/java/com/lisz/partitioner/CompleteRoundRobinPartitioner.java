package com.lisz.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

//这里由于KafkaProducer的缘故，在短时间内过来很多消息的话，还是做不到完全的RoundRobin
public class CompleteRoundRobinPartitioner implements Partitioner {
    private AtomicInteger atomicCount = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int n = cluster.availablePartitionsForTopic(topic).size();
        if (keyBytes == null) {
            int count = atomicCount.getAndIncrement();
            return (count & Integer.MAX_VALUE) % n;
        }
        return Utils.toPositive(Utils.murmur2(keyBytes)) % n;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
