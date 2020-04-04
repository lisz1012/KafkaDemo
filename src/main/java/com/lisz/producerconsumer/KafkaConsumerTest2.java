package com.lisz.producerconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/*
不特别指定partition的时候，Producer发送消息的时候会让不同的Partitions负载均衡，均分各个消息；consumer指定了哪一个组的时候，同组的consumers
对各个partitions进行分工，倾向于每人一个partition。这是Kafka的Consumer分组策略。当不指定consumer的组的时候（如下），consumers对于Partitions的
自动均衡就没有了，这时候就得手动assign partitions，这时候还能同时指定从哪个offset开始读，多开几个相同的consumer实例也不会互相干扰
 */

public class KafkaConsumerTest2 {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        List<TopicPartition> partitions = Arrays.asList(new TopicPartition("topic01", 0));
        consumer.assign(partitions);
        //consumer.seekToBeginning(partitions);//从头开始消费
        //consumer.seek(partitions.get(0), 1);//从指定的offset=1的位置开始消费
        consumer.seek(new TopicPartition("topic01", 0), 2);//从指定的topic01的partition 0 的 offset=2的位置开始消费，这里的第二个参数要是上面assign的时候指定的：new TopicPartition("topic01", 0)
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
    }
}