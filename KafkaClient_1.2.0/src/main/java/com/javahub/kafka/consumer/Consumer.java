package com.javahub.kafka.consumer;

import kafka.consumer.KafkaStream;
import org.apache.log4j.Logger;
import kafka.consumer.ConsumerIterator;

public class Consumer implements Runnable {
	private KafkaStream<byte[], byte[]> stream;
	private int threadNumber;
	private EventConsumer eventConsumer;
	private static final Logger logger = Logger.getLogger(Consumer.class);

	public Consumer(KafkaStream<byte[], byte[]> stream, int threadNumber, EventConsumer eventConsumer) {
		this.threadNumber = threadNumber;
		this.stream = stream;
		this.eventConsumer = eventConsumer;
		Thread.currentThread().setName("Thread_" + threadNumber);
	}

	public void run() {
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			String message = new String(it.next().message());
			eventConsumer.onReceive(message, threadNumber);
		}
		logger.info("Shutting down Thread: " + threadNumber);
	}
}