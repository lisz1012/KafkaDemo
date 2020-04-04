package com.lisz.dml;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;

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
        //查看Topic列表
        ListTopicsResult result = adminClient.listTopics();
        Set<String> names = result.names().get();
        names.forEach(n->{
            System.out.println(n);
        });
        // Close
        adminClient.close();
    }
}
