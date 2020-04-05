package com.lisz.serialize;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
/*
字符串转成字节数组发出去，对面Consumer的StringDeserializer是可以正确解读的
 */
public class KafkaProducerForByteArrayOfStringTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class); // 写.getName()的话会在org.apache.kafka.common.config.ConfigDef 719行被Class.forName转成Class
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class);

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);

        for (int i = 0; i < 9; i++) {
            producer.send(new ProducerRecord<byte[], byte[]>("topic01", ("lisz" + i).getBytes(), ("李书征" + i).getBytes()));
            TimeUnit.SECONDS.sleep(3);
        }

        producer.close();
    }
}
