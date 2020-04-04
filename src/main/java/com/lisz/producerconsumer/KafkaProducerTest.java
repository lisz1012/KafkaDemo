package com.lisz.producerconsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
/*
kafka当前最新版本2.4.0合入的一个KIP-480，它的核心逻辑就是当存在无key的序列消息时，我们消息发送的分区优先保持粘连，如果当前分区下的batch已经满了或者
linger.ms延迟时间已到开始发送，就会重新启动一个新的分区（逻辑还是按照Round-Robin模式）
https://segmentfault.com/a/1190000020515457
 */
public class KafkaProducerTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 9; i++) {
            //还可以用下面的构造方法指定partition，有key的情况用hash，无key的情况, 默认RoundRobinPartitioner一小段时间内会把所有消息发给同一个随机的partition，时间间隔足够长才可以得到完全的Round-Robin效果。新版本的优化
            //producer.send(new ProducerRecord<String, String>("topproducer.send(new ProducerRecord<String, String>("topic01", "lisz" + i));ic01", "lisz" + i, "李书征" + i));
            //producer.send(new ProducerRecord<String, String>("topic01", "李书征" + i));
            producer.send(new ProducerRecord<String, String>("topic01", "lisz" + i));
            TimeUnit.SECONDS.sleep(5);
        }

        producer.close();
    }
}
