package com.lisz.producerconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;
// 同一个组之内的消息是有序的，有consumer加入或者退出，可能引起各个consumer对于topic下各个partition的rebalance。有可能一个consumer负责两个partition
public class KafkaConsumerTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");//可以用到组管理机制

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Pattern.compile("^topic.*"));//可以用到组管理机制
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) { //从队列中取到了数据
                Iterable<ConsumerRecord<String, String>> records = consumerRecords.records("topic01");//也可以用consumerRecords.iterator()
                records.forEach(r->{
                    System.out.println(String.format("Topic: %s Partition: %s Offset: %s Key: %s Value: %s Timestamp: %s",
                            r.topic(), r.partition(), r.offset(), r.key(), r.value(), r.timestamp()));
                });
            }
        }

        //consumer.close();
    }
}
/* 同一个分区中消息是有序的，不同的分区综合起来看消息是无序的
Topic: topic01 Partition: 0 Offset: 12 Key: lisz1 Value: 李书征1 Timestamp: 1585974667802
Topic: topic01 Partition: 0 Offset: 13 Key: lisz2 Value: 李书征2 Timestamp: 1585974667802
Topic: topic01 Partition: 0 Offset: 14 Key: lisz5 Value: 李书征5 Timestamp: 1585974667802
Topic: topic01 Partition: 0 Offset: 15 Key: lisz8 Value: 李书征8 Timestamp: 1585974667803
Topic: topic01 Partition: 2 Offset: 8 Key: lisz3 Value: 李书征3 Timestamp: 1585974667802
Topic: topic01 Partition: 2 Offset: 9 Key: lisz6 Value: 李书征6 Timestamp: 1585974667802
Topic: topic01 Partition: 2 Offset: 10 Key: lisz9 Value: 李书征9 Timestamp: 1585974667803
Topic: topic01 Partition: 1 Offset: 9 Key: lisz0 Value: 李书征0 Timestamp: 1585974667795
Topic: topic01 Partition: 1 Offset: 10 Key: lisz4 Value: 李书征4 Timestamp: 1585974667802
Topic: topic01 Partition: 1 Offset: 11 Key: lisz7 Value: 李书征7 Timestamp: 1585974667803
 */