## Kafka-Eagle监控的安装与配置

1. 官网下载：http://download.kafka-eagle.org  
2. 传到某一台机器上。在哪个节点上装都可以，装一个就行  
3. `tar xf kafka-eagle-bin-1.4.6.tar`
4. `cd kafka-eagle-bin-1.4.6/`  
5. 进入目录后再次解压：`tar xf kafka-eagle-web-1.4.6-bin.tar.gz -C /usr/local`  
6. 进入`~/.bashrc`配置环境变量：
   ```
   KE_HOME=/usr/local/kafka-eagle
   JAVA_HOME=/usr/java/latest
   PATH=$PATH:$JAVA_HOME/bin:KE_HOME/bin:.
   ...
   export KE_HOME
   ```
7. `source ~/.bashrc`  
8. 修改 KE_HOME/conf/system.properties:
   ```
   ######################################
   # multi zookeeper & kafka cluster list
   ######################################
   kafka.eagle.zk.cluster.alias=cluster1
   cluster1.zk.list=Kafka_1:2181,Kafka_2:2181,Kafka_3:2181
   #cluster2.zk.list=xdn10:2181,xdn11:2181,xdn12:2181
   ```
   ```
   ######################################
   # kafka offset storage
   ######################################
   cluster1.kafka.eagle.offset.storage=kafka
   #cluster2.kafka.eagle.offset.storage=zk
   ```
   ```
   ######################################
   # kafka metrics, 30 days by default
   ######################################
   kafka.eagle.metrics.charts=true #这里改成true的话要修改Kafka的配置，启动JMX端口
   kafka.eagle.metrics.retain=30  
   ```
   ```
   ######################################
   # kafka sasl authenticate
   ######################################
   ...
   ...
   #cluster2.kafka.eagle.sasl.enable=false
   #cluster2.kafka.eagle.sasl.protocol=SASL_PLAINTEXT
   #cluster2.kafka.eagle.sasl.mechanism=PLAIN
   #cluster2.kafka.eagle.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka" password="kafka-eagle";
   #cluster2.kafka.eagle.sasl.client.id=
   #cluster2.kafka.eagle.sasl.cgroup.enable=false
   #cluster2.kafka.eagle.sasl.cgroup.topics
   ```
   ```
   ######################################
   # kafka sqlite jdbc driver address
   ######################################
   #kafka.eagle.driver=org.sqlite.JDBC
   #kafka.eagle.url=jdbc:sqlite:/hadoop/kafka-eagle/db/ke.db
   #kafka.eagle.username=root
   #kafka.eagle.password=www.kafka-eagle.org
   ```
   ```
   ######################################
   # kafka mysql jdbc driver address
   ######################################
   kafka.eagle.driver=com.mysql.jdbc.Driver
   kafka.eagle.url=jdbc:mysql://127.0.0.1:3306/userke?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull
   kafka.eagle.username=root
   kafka.eagle.password=^abc123$
   ```
   userke数据库会在Kafka启动的时候自动创建，还要确保MySQL可以远程访问
9. 在各台Kafka broker上面修改`KAFKA_HOME/bin/kafka-server-start.sh`  
   ```
   if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
       export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
       export JMX_PORT="7788"
   fi
   ```
   加入`export JMX_PORT="7788"`  
10. 各台Kafka broker启动（或重启）  
11. 如果不是JDK1.8而是12、13等版本可能需要把/usr/local/kafka-eagle/kms/bin/catalina.sh中的JAVA_OPTS修改为：
   ```
    JAVA_OPTS="-server -Xms2048M -Xmx2048M -Xmn1024M -Xss512k -XX:SurvivorRatio=8 -XX:MetaspaceSize=96M -XX:+UseBiasedLocking -XX:CMSInitiatingOccupancyFraction=75 -XX:+DisableExplicitGC -XX:MaxTenuringThreshold=15 -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:LargePageSizeInBytes=128m -XX:+UseCMSInitiatingOccupancyOnly -Djava.awt.headless=true"
   ```
   因为有些JVM参数已经废弃了，启动的时候会出错。报错日志在`/usr/local/kafka-eagle/kms/logs/catalina.out` 中
11. `cd /usr/local/kafka-eagle/bin`  
12. `chomd 0755 ./ke.sh`  
13. `./ke.sh start` 启动之后会在`127.0.0.1`上建立一个名为userke的数据库 (安装MySQL：https://www.jianshu.com/p/276d59cbc529  <----- 很详细！！)
   