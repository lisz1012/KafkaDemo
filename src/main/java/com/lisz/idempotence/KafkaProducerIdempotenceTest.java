package com.lisz.idempotence;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
/*
HTTP/1.1中对于幂等性的定义是：一次和多次请求某一个资源，对于资源被身应该具有同样的结果（网络超时等问题
除外）。也就是说，任意多次执行对资源本身所产生的影响均与依次执行所产生的影响相同。具体在Kafka中，就是指
消息既不会消失也不会重复。实现幂等的关键点就是服务端可以区分请求是否重复，从而过滤掉重复的请求。要区分重
复请求有两点：
1。唯一的标识：例如支付请求中的订单号
2。记录下已处理过请求的标识：当收到新的请求时，用新请求中的标识和和处理过的记录做比较，如果已存在了，
说明是producer重复发送，拒绝

幂等又称exactly once。要停止多次处理消息，必须将其持久化到Kafka Topic中有且只有一次。在初始化期间，
Kafka会给生产者生成一个唯一的ID，称为producer ID或者PID。PID和序列号绑在一起，然后发给broker。由于
序列号从零开始并且单调递增。因此，仅当消息的序列号比该PID / TopicPartition中最后提交的消息正好大1时，
broker才会接受消息。如果不是这种情况，则Broker使生产者重新发送了该消息。
enable.idempotence=false 默认
注意：在使用幂等性的时候，要求必须开启retries大于0，且acks=all
 */
public class KafkaProducerIdempotenceTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // 写.getName()的话会在org.apache.kafka.common.config.ConfigDef 719行被Class.forName转成Class
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //设置acks和retries.ProducerConfig.RETRIES_CONFIG这个参数不包含第一次的发送，如果尝试发送3次失败，则放弃。不设置retry的话就重试Integer.MAX_VALUE次
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 30);
        //故意将检测超时的时间设置为1ms
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5);//默认30000 ms, 这里故意让它超时
        //保证严格有序.如果有一个发送不成功就阻塞，直到成功为止
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        //设置开启幂等写
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);//默认不开启幂等，consumer会收到4条消息；开启之后只收到一条

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        producer.send(new ProducerRecord<String, String>("topic01", "ack", "test - ack"));
        producer.flush();

        producer.close();
    }
}
