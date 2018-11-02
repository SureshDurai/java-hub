package com.javahub.kafka.consumer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class EventConsumer {

	private Map<String, ConsumerGroup> consumerGrpMap = new HashMap<String, ConsumerGroup>();
	private String groupId;
	private String topic;
	private int threads;

	public EventConsumer(String groupId, String topic, int threads) {
		this.groupId = groupId;
		this.topic = topic;
		this.threads = threads;
	}

	public void startConsumer() throws Exception {
		ConsumerGroup consumerGrp = new ConsumerGroup(groupId, topic);
		consumerGrp.run(threads, this);
		consumerGrpMap.put(groupId, consumerGrp);
	}

	public void shutdownConsumerGroup(String groupId) {
		ConsumerGroup cg = consumerGrpMap.get(groupId);
		if (cg != null) {
			cg.shutdown();
		}
	}

	public void shutdownAllConsumers() {
		Set<String> groupIdSet = consumerGrpMap.keySet();
		Iterator<String> iter = groupIdSet.iterator();
		while (iter.hasNext()) {
			shutdownConsumerGroup(iter.next());
		}
	}

	public abstract void onReceive(String message, int threadId);
}
