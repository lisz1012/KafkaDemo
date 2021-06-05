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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerThreadPool {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-02:9092,hadoop-03:9092,hadoop-04:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 指定为earlist的时候重启，即便
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        List<TopicPartition> partitions = Arrays.asList(new TopicPartition("topic02", 0));
        consumer.assign(partitions);

        ExecutorService threadpool = Executors.newFixedThreadPool(8);

        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) { //从队列中取到了数据
                Iterable<ConsumerRecord<String, String>> records = consumerRecords.records("topic02");//也可以用consumerRecords.iterator()
                records.forEach(r->{


                    threadpool.submit(new Task(r, consumer));


//                    try {
//                        int time = 2;
//                        Thread.sleep(80 + time);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    System.out.println(String.format("%s processed Record from Topic: %s " +
//                                    "Partition: %s Offset: %s Key: %s Value: %s Timestamp: %s",
//                            Thread.currentThread().getName(), r.topic(), r.partition(), r.offset(),
//                            r.key(), r.value(), r.timestamp()));
//                    if (r.offset() == 105) {
//                        consumer.commitSync();
//                    }



                });
            }
            consumer.commitSync();
            //TimeUnit.SECONDS.sleep(1);Found no committed offset for partition topic02-0
        }
    }
}