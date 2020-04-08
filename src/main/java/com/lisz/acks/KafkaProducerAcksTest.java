package com.lisz.acks;

import com.lisz.serialize.Person;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducerAcksTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // 写.getName()的话会在org.apache.kafka.common.config.ConfigDef 719行被Class.forName转成Class
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //设置acks和retries.ProducerConfig.RETRIES_CONFIG这个参数不包含第一次的发送，如果尝试发送3次失败，则放弃。不设置retry的话就重试Integer.MAX_VALUE次
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        //故意将检测超时的时间设置为1ms
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10);//默认30000 ms

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        producer.send(new ProducerRecord<String, String>("topic01", "ack", "test - ack"));
        producer.flush();

        producer.close();
    }
}
