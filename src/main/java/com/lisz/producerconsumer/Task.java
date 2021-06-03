package com.lisz.producerconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Task implements Runnable {
	private ConsumerRecord<String, String> record;

	private static Random rand = new Random();

	public Task(ConsumerRecord<String, String> record) {
		this.record = record;
	}

	@Override
	public void run() {
		try {
			int time = rand.nextInt(30);
			Thread.sleep(time);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println(String.format("%s processed Record from Topic: %s " +
						"Partition: %s Offset: %s Key: %s Value: %s Timestamp: %s",
				Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(),
				record.key(), record.value(), record.timestamp()));
	}
}
