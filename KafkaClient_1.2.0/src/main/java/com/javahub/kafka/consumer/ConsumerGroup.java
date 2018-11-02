package com.javahub.kafka.consumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.javahub.kafka.utils.EventConstants;

public class ConsumerGroup {
	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;
	private static final Logger logger = Logger.getLogger(ConsumerGroup.class);

	public ConsumerGroup(String a_groupId, String a_topic) throws Exception {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(a_groupId));
		this.topic = a_topic;
	}

	public void shutdown() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				logger.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			logger.error("Interrupted during shutdown, exiting uncleanly");
		}
	}

	public void run(int a_numThreads, EventConsumer eventConsumer) throws Exception {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(a_numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		// now launch all the threads
		//
		executor = Executors.newFixedThreadPool(a_numThreads);

		// now create an object to consume the messages
		//
		int threadNumber = 0;
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new Consumer(stream, threadNumber, eventConsumer));
			threadNumber++;
		}
	}

	private static ConsumerConfig createConsumerConfig(String groupId) throws Exception {
		logger.info(" LOCATION ==" + System.getProperty("user.dir"));
		Properties props = new Properties();
		props.load(new FileInputStream(new File(EventConstants.PROPERTY_FILE_PATH)));

		Properties consumerProps = new Properties();
		logger.info("[" + props.getProperty("kafka.consumer.zookeeper.connect") + "]");
		consumerProps.put("zookeeper.connect", props.getProperty("kafka.consumer.zookeeper.connect"));
		consumerProps.put("group.id", groupId);
		consumerProps.put("zookeeper.session.timeout.ms",
				props.getProperty("kafka.consumer.zookeeper.session.timeout.ms"));
		consumerProps.put("zookeeper.sync.time.ms", props.getProperty("kafka.consumer.zookeeper.sync.time.ms"));
		consumerProps.put("auto.commit.interval.ms", props.getProperty("kafka.consumer.auto.commit.interval.ms"));
		consumerProps.put("controlled.shutdown.enable", props.getProperty("kafka.controlled.shutdown.enable"));

		return new ConsumerConfig(consumerProps);
	}

}