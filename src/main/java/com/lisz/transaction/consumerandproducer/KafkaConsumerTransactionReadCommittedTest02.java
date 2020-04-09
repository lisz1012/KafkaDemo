package com.lisz.transaction.consumerandproducer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

// 同一个组之内的消息是有序的，有consumer加入或者退出，可能引起各个consumer对于topic下各个partition的rebalance。有可能一个consumer负责两个partition
public class KafkaConsumerTransactionReadCommittedTest02 {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g2");//可以用到组管理机制：组内负载均衡，组间广播

        //设置消费者的消费事务的隔离级别为read_committed，确保事务
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("topic02"));//可以用到组管理机制：组内负载均衡，组间广播
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            System.out.println(consumerRecords.isEmpty());
            if (!consumerRecords.isEmpty()) { //从队列中取到了数据
                Iterable<ConsumerRecord<String, String>> records = consumerRecords.records("topic02");//也可以用consumerRecords.iterator()
                System.out.println("Has next: " + records.iterator().hasNext());
                records.forEach(r->{
                    System.out.println(String.format("Topic: %s Partition: %s Offset: %s Key: %s Value: %s Timestamp: %s",
                            r.topic(), r.partition(), r.offset(), r.key(), r.value(), r.timestamp()));
                });
            }
        }

        //consumer.close();
    }
}
/* 一共有3个partition，所以分了三波poll出来
Has next: true
Topic: topic02 Partition: 1 Offset: 42 Key: lisz0 Value: 李书征 - lisz!! Timestamp: 1586413916942
Topic: topic02 Partition: 1 Offset: 43 Key: lisz4 Value: 李书征 - lisz!! Timestamp: 1586413916985
Topic: topic02 Partition: 1 Offset: 44 Key: lisz7 Value: 李书征 - lisz!! Timestamp: 1586413916995
false
Has next: true
Topic: topic02 Partition: 2 Offset: 39 Key: lisz3 Value: 李书征 - lisz!! Timestamp: 1586413917507
Topic: topic02 Partition: 2 Offset: 40 Key: lisz6 Value: 李书征 - lisz!! Timestamp: 1586413917526
Topic: topic02 Partition: 2 Offset: 41 Key: lisz9 Value: 李书征 - lisz!! Timestamp: 1586413917758
false
Has next: true
Topic: topic02 Partition: 0 Offset: 51 Key: lisz1 Value: 李书征 - lisz!! Timestamp: 1586413917778
Topic: topic02 Partition: 0 Offset: 52 Key: lisz2 Value: 李书征 - lisz!! Timestamp: 1586413917986
Topic: topic02 Partition: 0 Offset: 53 Key: lisz5 Value: 李书征 - lisz!! Timestamp: 1586413917995
Topic: topic02 Partition: 0 Offset: 54 Key: lisz8 Value: 李书征 - lisz!! Timestamp: 1586413918004
 */