package com.lisz.serialize;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducerForObjectTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class); // 写.getName()的话会在org.apache.kafka.common.config.ConfigDef 719行被Class.forName转成Class
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ObjectSerializer.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class);

        KafkaProducer<Integer, Person> producer = new KafkaProducer<Integer, Person>(props);

        for (int i = 0; i < 9; i++) {
            Person person = new Person(i, "lisz" + i);
            producer.send(new ProducerRecord<Integer, Person>("topic01", i, person));
            TimeUnit.SECONDS.sleep(3);
        }

        producer.close();
    }
}
