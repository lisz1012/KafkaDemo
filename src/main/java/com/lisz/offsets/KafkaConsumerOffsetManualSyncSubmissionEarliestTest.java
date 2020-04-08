package com.lisz.offsets;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// 同一个组之内的消息是有序的，有consumer加入或者退出，可能引起各个consumer对于topic下各个partition的rebalance。有可能一个consumer负责两个partition
public class KafkaConsumerOffsetManualSyncSubmissionEarliestTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g4");//这里与KafkaConsumerOffsetTest中的设置不同，是g2，是一个崭新的组，没有消费过topic02 （每次测试要设置一个新组）
        //这个配置只管着这个Consumer第一次消费这个topic（系统还没有偏移量）的时候从最早的开始读。不是第一次消费这个topic的话，与latest行为相同，在这之后都会从错过的第一条消息开始读，因为有offset自动提交
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //手动提交偏移量
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("topic02"));//可以用到组管理机制：组内负载均衡，组间广播
        while (true) {
            //poll里面会fetch消息，Fetcher的1541行：nextFetchOffset = lastRecord.offset() + 1; 偏移量递增；而在685、420、839行会更新本地偏移量
            //而提交到服务器的配置是通过enable.auto.commit=true和auto.commit.interval.ms=5000来配置的
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) { //从队列中取到了数据
                Iterable<ConsumerRecord<String, String>> records = consumerRecords.records("topic02");//也可以用consumerRecords.iterator()
                records.forEach(r->{
                    String topic = r.topic();
                    int partition = r.partition();
                    long offset = r.offset();
                    String key = r.key();
                    String value = r.value();
                    long timestamp = r.timestamp();
                    System.out.println(String.format("Topic: %s Partition: %s Offset: %s Key: %s Value: %s Timestamp: %s",
                            topic, partition, offset, key, value, timestamp));
                    consumer.commitSync();
                });
            }
        }

        //consumer.close();
    }
}
/* 读取成功，而且日志中出现了
auto.offset.reset = earliest
以及
Resetting offset for partition topic02-1 to offset 0.
的日志
 */