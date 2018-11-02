package com.javahub.kafka.publisher;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class Publisher_Test {
	public static void main(String[] args) throws Exception {
		// System.out.println( "STARTING .....!!!!!!!! " );
		// EventPublisher publisher = new EventPublisher();
		// for (int i = 0; i< 100 ; i++){
		// publisher.publishEvent("qDDEvent", "{TEST MESSAGE}_"+i);
		// }
		// System.out.println( "Message Sent !!!!!!!! " );
		Publisher_Test p = new Publisher_Test();
		for (int i = 0; i < 250; i++) {
			p.getBlackListMap();
		}

	}

	static Date dateWithoutTime;
	static ConcurrentHashMap<String, List<String>> blackListMap;

	private ConcurrentHashMap<String, List<String>> getBlackListMap() throws Exception {
		if (dateWithoutTime == null) {
			SimpleDateFormat currentDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			dateWithoutTime = currentDateFormat.parse(currentDateFormat.format(new Date()));
		}
		SimpleDateFormat currentDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date currentDate = currentDateFormat.parse(currentDateFormat.format(new Date()));

		System.out.println("isBlackListed :: Calling ...." + currentDate.compareTo(dateWithoutTime));
		if (currentDate.compareTo(dateWithoutTime) != 0 || blackListMap == null
				|| (!blackListMap.containsKey("TEST"))) {
			System.out.println("isBlackListed :: Creating BlackList [START] ...Should be only called once in a day");
			blackListMap = new ConcurrentHashMap<String, List<String>>();
			blackListMap.put("TEST", new ArrayList<String>());

		}
		return blackListMap;
	}
}
