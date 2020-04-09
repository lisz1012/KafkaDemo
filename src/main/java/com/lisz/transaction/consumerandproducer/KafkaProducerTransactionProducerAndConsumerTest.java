package com.lisz.transaction.consumerandproducer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class KafkaProducerTransactionProducerAndConsumerTest {
    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> producer = buildKafkaProducer();
        KafkaConsumer<String, String> consumer = buildKafkaConsumer("g1");

        producer.initTransactions();
        consumer.subscribe(Arrays.asList("topic01"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                try {
                    producer.beginTransaction();
                    while (iterator.hasNext()) {
                        ConsumerRecord<String, String> record = iterator.next();
                        //存储元数据
                        offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                        producer.send(new ProducerRecord<>("topic02", record.key(), record.value() + " - lisz!!"));
                        producer.flush();
                    }
                    producer.sendOffsetsToTransaction(offsets, "g1");//提交消费者的偏移量
                    producer.commitTransaction();
                } catch (Exception e) {
                    System.err.println("出现错误了： " + e);
                    producer.abortTransaction();
                } finally {
                    //producer.close();
                }
            }
        }
    }

    public static KafkaConsumer<String, String> buildKafkaConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);//可以用到组管理机制：组内负载均衡，组间广播

        //设置消费者的消费事务的隔离级别为read_committed，确保事务
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        //必须关闭消费者的offset自动提交，必须等生产者那边都处理完成才可以手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<String, String>(props);
    }

    public static KafkaProducer<String, String> buildKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // 写.getName()的话会在org.apache.kafka.common.config.ConfigDef 719行被Class.forName转成Class
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //必须配置事物ID，且必须是唯一的
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "Transactional-ID-" + UUID.randomUUID());
        //配置Kafka的批处理大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        //等待5ms，如果batch中的数据不足1024个，仍然不够就发送了
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        //配置Kafka的重试机制和幂等
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 100000);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // Allowing retries without setting max.in.flight.requests.per.connection to 1 will potentially change the ordering of records
        // because if two batches are sent to a single partition, and the first fails and is retried but the second succeeds, then the
        // records in the second batch may appear first.
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        return new KafkaProducer<>(props);
    }
}
