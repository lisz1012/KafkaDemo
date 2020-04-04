package com.lisz.dml;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

// 必须将Kafka_1,Kafka_2,Kafka_3的IP和主机名的映射配置在操作系统中：/etc/hosts
public class KafkaTopicDML {
    public static void main(String[] args) throws Exception {
        // 1. 创建KafkaAdminClient类
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");

        KafkaAdminClient adminClient = (KafkaAdminClient)KafkaAdminClient.create(props);

        //创建Topic
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(new NewTopic("Topic03", 3, (short)3)));
        // KafkaFuture.get() 异步变同步，这里如果Topic03已经创建了，就会报错；而如果没有这一句，主线程早早就结束了，会直接打印topic名称，然后并无报错
        createTopicsResult.all().get();

        //查看Topic列表
        ListTopicsResult result = adminClient.listTopics();
        Set<String> names = result.names().get();
        names.forEach(n->{
            System.out.println(n);
        });

        // Topic的删除, 也是异步的, 变同步的时候同样要 .all().get()
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList("topic02", "Topic03"));
        deleteTopicsResult.all().get();

        // 查看topic详情
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList("topic01"));
        Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
        topicDescriptionMap.entrySet().forEach(e->{
            System.out.println(e);
        });
        // Close
        adminClient.close();
    }
}
