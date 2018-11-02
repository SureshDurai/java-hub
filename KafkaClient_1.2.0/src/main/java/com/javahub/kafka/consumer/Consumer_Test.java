package com.javahub.kafka.consumer;

public class Consumer_Test extends EventConsumer {

	public Consumer_Test(String groupId, String topic, int threads) {
		super(groupId, topic, threads);
	}

	public static void main(String[] args) throws Exception {
		String groupId = "DDEvent";
		String topic = "qDDEvent";
		int threads = 6;
		EventConsumer ec = new Consumer_Test(groupId, topic, threads);
		ec.startConsumer();
		// Call shutdown if you want to shutdown your consumer.
		// ec.shutdownAllConsumers();
	}

	/**
	 * This method will be called every time a message is sent to respective
	 * queue and this consumer is running.
	 */
	@Override
	public void onReceive(String message, int threadId) {
		System.out.println(" MEssage Recieved ::: " + message + " for thread id " + threadId);
		// ADD YOU METHOD DETAILS FOR PROCESSING THE MESSAGES....
	}

}
