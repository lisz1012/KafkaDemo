package com.lisz.partitioner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/*
使用自定义分区器
 */
public class KafkaProducerWithPartitionerConfigTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // 写.getName()的话会在org.apache.kafka.common.config.ConfigDef 719行被Class.forName转成Class
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SinglePartitioner.class);
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CompleteRandomPartitioner.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CompleteRoundRobinPartitioner.class);

        //分析源码可知：下面构造方法如果指定StringSerializer的话会覆盖上面props中的设定
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 9; i++) {
            producer.send(new ProducerRecord<String, String>("topic01", "lisz" + i, "lisz" + i));//有key也不会覆盖props中的Partitioner
            TimeUnit.SECONDS.sleep(2);
        }

        producer.close();
    }
}
