package com.javahub.kafka.publisher;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.javahub.kafka.utils.EventConstants;

public class EventPublisher {

	private static Properties props = new Properties();
	private Properties kafkaProps = new Properties();
	private Producer<String, String> producer;

	private static final Logger logger = Logger.getLogger(EventPublisher.class);

	static {
		try {
			logger.info(" LOCATION ==" + System.getProperty("user.dir"));
			props.load(new FileInputStream(new File(EventConstants.PROPERTY_FILE_PATH)));
		} catch (Exception ex) {
			ex.printStackTrace();
			;
		}
	}

	public EventPublisher() throws Exception {
		logger.info("Kafka Server : " + props.get("kafka.publisher.bootstrap.servers"));

		kafkaProps.put("bootstrap.servers", props.get("kafka.publisher.bootstrap.servers"));
		kafkaProps.put("batch.size", props.get("kafka.publisher.batch.size"));
		kafkaProps.put("linger.ms", props.get("kafka.publisher.linger.ms"));
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(kafkaProps);
	}

	public void publishEvent(String topicName, String jsonMessage) throws Exception {
		producer.send(new ProducerRecord<String, String>(topicName, jsonMessage));
	}

	public void close() {
		producer.close();
	}

}
