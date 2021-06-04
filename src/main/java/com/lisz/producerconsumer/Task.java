package com.lisz.producerconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Task implements Runnable {
	private ConsumerRecord<String, String> record;
	private KafkaConsumer<String, String> consumer;

	private static Random rand = new Random();

	public Task(ConsumerRecord<String, String> record, KafkaConsumer<String, String> consumer) {
		this.record = record;
		this.consumer = consumer;
	}

	@Override
	public void run() {
		try {
			int time = rand.nextInt(40);
			Thread.sleep(80 + time);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println(String.format("%s processed Record from Topic: %s " +
						"Partition: %s Offset: %s Key: %s Value: %s Timestamp: %s",
				Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(),
				record.key(), record.value(), record.timestamp()));
		//if (record.offset() == 35) {
			//consumer.commitSync();
		//}
	}
}
