package com.lisz.transaction;

import com.lisz.serialize.Person;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class KafkaProducerTransactionProducerOnlyTest {
    public static void main(String[] args) throws Exception {
        KafkaProducer producer = buildKafkaProducer();

        // 1. 初始化一个事务
        producer.initTransactions();

        try {
            // 2. 开始事务
            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                //if (i == 8) {
                //    int j = 8 / 0;
                //} //注掉之后10条消息就在两边都全部打印出来了
                producer.send(new ProducerRecord<String, String>("topic01", "lisz" + i, "李书征"));
                producer.flush();
            }
            // 3。 提交事务
            producer.commitTransaction();
        } catch (Exception e) {
            System.err.println("出现错误了： " + e);//会打印出来，且read_committed消费者那边没有任何输出，他读不到未提交的数据；而read_uncommitted消费者那边，如果不是同一个consumer group则可以读到前8个消息；如果是同一个consumer group，则能读到他所负责的分区的数据
            // 4。 中止事务
            producer.abortTransaction();
        } finally {
            producer.close();
        }
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
        // records in the second batch may appear first.这里保证了记录的严格有序
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        return new KafkaProducer<>(props);
    }
}
