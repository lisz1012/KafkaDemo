package com.lisz.producerconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConsumerSingleThread {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-02:9092,hadoop-03:9092,hadoop-04:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 指定为earlist的时候，仅仅第一次启动的时候从头读取，易弄错，以后再启动就跟latest没区别了
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        List<TopicPartition> partitions = Arrays.asList(new TopicPartition("topic02", 0));
        consumer.assign(partitions);

        ExecutorService threadpool = Executors.newFixedThreadPool(6);

        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) { //从队列中取到了数据，配置中可以指定消息个数的上限
                Iterable<ConsumerRecord<String, String>> records = consumerRecords.records("topic02");//也可以用consumerRecords.iterator()
                List<Person> people = new ArrayList<>();
                records.forEach(r->{
                    people.add(new Person(r.offset(), r.value()));
                });
                people.forEach(System.out::println);
                //提交当前这整个一poll出来之后的偏移量，而不是某一个的，所以不能过早提交，那样会丢数据
                consumer.commitSync();
            }



            //TimeUnit.SECONDS.sleep(1);Found no committed offset for partition topic02-0
        }
    }
}