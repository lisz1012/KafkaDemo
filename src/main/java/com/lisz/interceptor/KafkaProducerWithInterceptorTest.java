package com.lisz.interceptor;

import com.lisz.serialize.Person;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducerWithInterceptorTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // 写.getName()的话会在org.apache.kafka.common.config.ConfigDef 719行被Class.forName转成Class
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyProducerInterceptor.class.getName());//Arrays.asList(MyProducerInterceptor.class)); //.class的形式必须写成List

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 9; i++) {
            producer.send(new ProducerRecord<String, String>("topic01", "lisz" + i, "李书征"));
            TimeUnit.SECONDS.sleep(3);
        }

        producer.close();
    }
}
